//
// Copyright (c) 2022 ZettaScale Technology
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ZettaScale Zenoh Team, <zenoh@zettascale.tech>
//

use async_std::sync::{Arc, Mutex};
use async_trait::async_trait;
use log::{debug, error, trace, warn};
use rusqlite::params;
use rusqlite::Connection;
use rusqlite::OpenFlags;
use std::collections::HashMap;
use async_std::fs;
use std::path::PathBuf;
use zenoh::buffers::{reader::HasReader, writer::HasWriter};
use zenoh::prelude::*;
use zenoh::properties::Properties;
use zenoh::time::Timestamp;
use zenoh::Result as ZResult;
use zenoh_backend_traits::config::{StorageConfig, VolumeConfig};
use zenoh_backend_traits::*;
use zenoh_codec::{RCodec, WCodec, Zenoh080};
use zenoh_core::{bail, zerror};
use zenoh_plugin_trait::{plugin_long_version, plugin_version, Plugin};
use zenoh_util::zenoh_home;

/// The environment variable used to configure the root of all storages managed by this sqlite backend.
pub const SCOPE_ENV_VAR: &str = "ZENOH_BACKEND_SQLITE_ROOT";

/// The default root (whithin zenoh's home directory) if the ZENOH_BACKEND_SQLITE_ROOT environment variable is not specified.
pub const DEFAULT_ROOT_DIR: &str = "zenoh_backend_sqlite";

// Properties used by the Backend
//  - None

// Properties used by the Storage
pub const PROP_STORAGE_DIR: &str = "dir";
pub const PROP_STORAGE_READ_ONLY: &str = "read_only";
pub const PROP_STORAGE_ON_CLOSURE: &str = "on_closure";

// Special key for None (when the prefix being stripped exactly matches the key)
pub const NONE_KEY: &str = "@@none_key@@";

pub(crate) enum OnClosure {
    DestroyDB,
    DoNothing,
}

pub struct SqliteBackend {}
zenoh_plugin_trait::declare_plugin!(SqliteBackend);

impl Plugin for SqliteBackend {
    type StartArgs = VolumeConfig;
    type Instance = VolumeInstance;

    const DEFAULT_NAME: &'static str = "sqlite_backend";
    const PLUGIN_VERSION: &'static str = plugin_version!();
    const PLUGIN_LONG_VERSION: &'static str = plugin_long_version!();

    fn start(_name: &str, _config: &Self::StartArgs) -> ZResult<Self::Instance> {
        // For some reasons env_logger is sometime not active in a loaded library.
        // Try to activate it here, ignoring failures.
        let _ = env_logger::try_init();
        debug!("Sqlite backend {}", Self::PLUGIN_LONG_VERSION);

        let root = if let Some(dir) = std::env::var_os(SCOPE_ENV_VAR) {
            PathBuf::from(dir)
        } else {
            let mut dir = PathBuf::from(zenoh_home());
            dir.push(DEFAULT_ROOT_DIR);
            dir
        };
        let mut properties = Properties::default();
        properties.insert("root".into(), root.to_string_lossy().into());
        properties.insert("version".into(), Self::PLUGIN_VERSION.into());

        let admin_status = HashMap::from(properties)
            .into_iter()
            .map(|(k, v)| (k, serde_json::Value::String(v)))
            .collect();
        Ok(Box::new(SqliteVolume { admin_status, root }))
    }
}

pub struct SqliteVolume {
    admin_status: serde_json::Value,
    root: PathBuf,
}

#[async_trait]
impl Volume for SqliteVolume {
    fn get_admin_status(&self) -> serde_json::Value {
        self.admin_status.clone()
    }

    fn get_capability(&self) -> Capability {
        Capability {
            persistence: Persistence::Durable,
            history: History::Latest,
            read_cost: 0,
        }
    }

    async fn create_storage(&self, config: StorageConfig) -> ZResult<Box<dyn Storage>> {
        let volume_cfg = match config.volume_cfg.as_object() {
            Some(v) => v,
            None => bail!("sqlite backed storages need volume-specific configurations"),
        };

        let read_only = match volume_cfg.get(PROP_STORAGE_READ_ONLY) {
            None | Some(serde_json::Value::Bool(false)) => false,
            Some(serde_json::Value::Bool(true)) => true,
            _ => {
                bail!(
                    "Optional property `{}` of sqlite storage configurations must be a boolean",
                    PROP_STORAGE_READ_ONLY
                )
            }
        };

        let on_closure = match volume_cfg.get(PROP_STORAGE_ON_CLOSURE) {
            Some(serde_json::Value::String(s)) if s == "destroy_db" => OnClosure::DestroyDB,
            Some(serde_json::Value::String(s)) if s == "do_nothing" => OnClosure::DoNothing,
            None => OnClosure::DoNothing,
            _ => {
                bail!(
                    r#"Optional property `{}` of sqlite storage configurations must be either "do_nothing" (default) or "destroy_db""#,
                    PROP_STORAGE_ON_CLOSURE
                )
            }
        };

        let mut db_path = match volume_cfg.get(PROP_STORAGE_DIR) {
            Some(serde_json::Value::String(dir)) => {
                let mut db_path = self.root.clone();
                db_path.push(dir);
                db_path
            }
            _ => {
                bail!(
                    r#"Required property `{}` for File System Storage must be a string"#,
                    PROP_STORAGE_DIR
                )
            }
        };

        fs::create_dir_all(&db_path).await?;
        db_path.push("zenoh.db");

        let conn = Connection::open_with_flags(
            db_path,
            OpenFlags::SQLITE_OPEN_READ_WRITE
                | OpenFlags::SQLITE_OPEN_CREATE
                | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )?;

        conn.execute("PRAGMA journal_mode=WAL;", [])?;
        conn.execute("PRAGMA synchronous=2;", [])?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS Parameters (key text unique, payload BLOB, info BLOB);",
            [],
        )?;

        let db = Arc::new(Mutex::new(Some(conn)));

        Ok(Box::new(SqliteStorage {
            config,
            on_closure,
            read_only,
            db,
        }))
    }

    fn incoming_data_interceptor(&self) -> Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>> {
        None
    }

    fn outgoing_data_interceptor(&self) -> Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>> {
        None
    }
}

struct SqliteStorage {
    config: StorageConfig,
    on_closure: OnClosure,
    read_only: bool,
    // Note: sqlite isn't thread-safe. See https://github.com/rust-sqlite/rust-sqlite/issues/404
    db: Arc<Mutex<Option<Connection>>>,
}

struct PayloadInfo {
    payload: Vec<u8>,
    info: Vec<u8>,
}

struct KeyInfo {
    key: String,
    info: Vec<u8>,
}

#[async_trait]
impl Storage for SqliteStorage {
    fn get_admin_status(&self) -> serde_json::Value {
        self.config.to_json_value()
    }

    // When receiving a PUT operation
    async fn put(
        &mut self,
        key: Option<OwnedKeyExpr>,
        value: Value,
        timestamp: Timestamp,
    ) -> ZResult<StorageInsertionResult> {
        // Get lock on DB
        let db_cell = self.db.lock().await;
        let db = db_cell.as_ref().unwrap();

        // Store the sample
        debug!(
            "Storing key, and value with timestamp: {:?} : {}",
            key, timestamp
        );
        if !self.read_only {
            // put payload and data_info in DB
            put_kv(db, key, value, timestamp)
        } else {
            warn!("Received PUT for read-only DB on {:?} - ignored", key);
            Err("Received update for read-only DB".into())
        }
    }

    // When receiving a DEL operation
    async fn delete(
        &mut self,
        key: Option<OwnedKeyExpr>,
        _timestamp: Timestamp,
    ) -> ZResult<StorageInsertionResult> {
        // Get lock on DB
        let db_cell = self.db.lock().await;
        let db = db_cell.as_ref().unwrap();

        debug!("Deleting key: {:?}", key);
        if !self.read_only {
            // delete file
            delete_kv(db, key)
        } else {
            warn!("Received DELETE for read-only DB on {:?} - ignored", key);
            Err("Received update for read-only DB".into())
        }
    }

    // When receiving a GET operation
    async fn get(
        &mut self,
        key: Option<OwnedKeyExpr>,
        _parameters: &str,
    ) -> ZResult<Vec<StoredData>> {
        // Get lock on DB
        let db_cell = self.db.lock().await;
        let db = db_cell.as_ref().unwrap();

        // Get the matching key/value
        debug!("getting key `{:?}` with parameters `{}`", key, _parameters);
        match get_kv(db, key.clone()) {
            Ok(Some((value, timestamp))) => Ok(vec![StoredData { value, timestamp }]),
            Ok(None) => Ok(vec![]),
            Err(e) => Err(format!("Error when getting key {:?} : {}", key, e).into()),
        }
    }

    async fn get_all_entries(&self) -> ZResult<Vec<(Option<OwnedKeyExpr>, Timestamp)>> {
        let mut result = Vec::new();

        let db_cell = self.db.lock().await;
        let db = db_cell.as_ref().unwrap();

        let mut stmt = db.prepare("SELECT (key, info) FROM Parameters")?;
        let iter = stmt.query_map([], |row| {
            Ok(KeyInfo {
                key: row.get(0)?,
                info: row.get(1)?,
            })
        })?;

        for item in iter {
            let item = item?;
            trace!("item ok");
            let (_, timestamp, deleted) = decode_data_info(&item.info)?;
            if !deleted {
                let key = if item.key == NONE_KEY {
                    None
                } else {
                    match OwnedKeyExpr::new(item.key.as_str()) {
                        Ok(ke) => Some(ke),
                        Err(e) => bail!("Invalid key in database: '{}' - {}", item.key, e),
                    }
                };
                result.push((key, timestamp));
            }
        }
        Ok(result)
    }
}

impl Drop for SqliteStorage {
    fn drop(&mut self) {
        async_std::task::block_on(async move {
            // Get lock on DB and take DB so we can drop it before destroying it
            // (avoiding Sqlite lock to be taken twice)
            let mut db_cell = self.db.lock().await;
            let db = db_cell.take().unwrap();

            // Flush all
            if let Err(err) = db.cache_flush() {
                warn!("Closing Sqlite storage, flush failed: {}", err);
            }

            // copy path for later use after DB is dropped
            let path = db.path().unwrap_or("").to_owned();

            // drop DB
            if let Err((_, err)) = db.close() {
                warn!("Closing Sqlite storage, flush failed: {}", err);
            }

            match self.on_closure {
                OnClosure::DestroyDB => {
                    debug!("Close Sqlite storage, destroying database {}", path);
                    if path.is_empty() {
                        error!("Failed to destroy database: empty path");
                    } else if let Err(e) = std::fs::remove_file(&path) {
                        error!("Failed to destroy database {}: {}", path, e);
                    }
                }
                OnClosure::DoNothing => {
                    debug!("Close Sqlite storage, keeping database {} as it is", path);
                }
            }
        });
    }
}

fn put_kv(
    db: &Connection,
    key: Option<OwnedKeyExpr>,
    value: Value,
    timestamp: Timestamp,
) -> ZResult<StorageInsertionResult> {
    trace!("Put key {:?} in {:?}", key, db);
    let data_info = encode_data_info(&value.encoding, &timestamp, false)?;
    let key = match key {
        Some(k) => k.to_string(),
        None => NONE_KEY.to_string(),
    };

    db.execute(
        "REPLACE INTO Parameters (key, payload, info) VALUES (?1,?2,?3);",
        params![
            key.as_str(),
            value.payload.contiguous(),
            data_info.as_slice()
        ],
    )?;

    Ok(StorageInsertionResult::Inserted)
}

fn delete_kv(db: &Connection, key: Option<OwnedKeyExpr>) -> ZResult<StorageInsertionResult> {
    trace!("Delete key {:?} from {:?}", key, db);
    let key = match key {
        Some(k) => k.to_string(),
        None => NONE_KEY.to_string(),
    };
    db.execute("DELETE FROM Parameters WHERE key = (?1);", [key.as_str()])?;
    Ok(StorageInsertionResult::Deleted)
}

fn get_kv(db: &Connection, key: Option<OwnedKeyExpr>) -> ZResult<Option<(Value, Timestamp)>> {
    trace!("Get key {:?} from {:?}", key, db);
    let key = match key {
        Some(k) => k.to_string(),
        None => NONE_KEY.to_string(),
    };

    let mut stmt = db.prepare("SELECT (payload, info) FROM Parameters WHERE key = (?1)")?;
    let res = stmt.query_row([key.as_str()], |row| {
        Ok(PayloadInfo {
            payload: row.get(0)?,
            info: row.get(1)?,
        })
    })?;

    let (encoding, timestamp, deleted) = decode_data_info(&res.info)?;
    trace!("first ok");
    if deleted {
        Ok(None)
    } else {
        Ok(Some((
            Value::new(res.payload.into()).encoding(encoding),
            timestamp,
        )))
    }
}

fn encode_data_info(encoding: &Encoding, timestamp: &Timestamp, deleted: bool) -> ZResult<Vec<u8>> {
    let codec = Zenoh080::new();
    let mut result = vec![];
    let mut writer = result.writer();

    // note: encode timestamp at first for faster decoding when only this one is required
    codec
        .write(&mut writer, timestamp)
        .map_err(|_| zerror!("Failed to encode data-info (timestamp)"))?;
    codec
        .write(&mut writer, deleted as u8)
        .map_err(|_| zerror!("Failed to encode data-info (deleted)"))?;
    codec
        .write(&mut writer, encoding)
        .map_err(|_| zerror!("Failed to encode data-info (encoding)"))?;
    Ok(result)
}

fn decode_data_info(buf: &[u8]) -> ZResult<(Encoding, Timestamp, bool)> {
    let codec = Zenoh080::new();
    let mut reader = buf.reader();
    let timestamp: Timestamp = codec
        .read(&mut reader)
        .map_err(|_| zerror!("Failed to decode data-info (timestamp)"))?;
    let deleted: u8 = codec
        .read(&mut reader)
        .map_err(|_| zerror!("Failed to decode data-info (deleted)"))?;
    let encoding: Encoding = codec
        .read(&mut reader)
        .map_err(|_| zerror!("Failed to decode data-info (timestamp)"))?;
    let deleted = deleted != 0;

    Ok((encoding, timestamp, deleted))
}
