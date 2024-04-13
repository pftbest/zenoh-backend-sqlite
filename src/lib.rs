//
// Copyright (c) 2022 ZettaScale Technology
// Copyright (c) 2024 Vadzim Dambrouski
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
//   Vadzim Dambrouski, <pftbest@gmail.com>
//

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use rusqlite::{params, Connection};
use tracing::{debug, error, trace};
use zenoh::buffers::buffer::SplitBuffer;
use zenoh::buffers::reader::HasReader;
use zenoh::buffers::writer::HasWriter;
use zenoh::key_expr::OwnedKeyExpr;
use zenoh::prelude::Sample;
use zenoh::time::Timestamp;
use zenoh::value::Value;
use zenoh_backend_traits::config::{StorageConfig, VolumeConfig};
use zenoh_backend_traits::{
    Capability, History, Persistence, Storage, StorageInsertionResult, StoredData, Volume,
    VolumeInstance, ZResult,
};
use zenoh_codec::{RCodec, WCodec, Zenoh080};
use zenoh_plugin_trait::{plugin_long_version, plugin_version, Plugin};
use zenoh_util::zenoh_home;

/// The environment variable used to configure the root of all storages managed by this sqlite backend.
const SCOPE_ENV_VAR: &str = "ZENOH_BACKEND_SQLITE_ROOT";

/// The default root (within zenoh's home directory) if the ZENOH_BACKEND_SQLITE_ROOT environment variable is not specified.
const DEFAULT_ROOT_DIR: &str = "zenoh_backend_sqlite";

const DEFAULT_FILE_NAME: &str = "zenoh.db";

const CREATE_TABLE_STMT: &str = r#"
CREATE TABLE IF NOT EXISTS zenoh_storage (
    key TEXT PRIMARY KEY,
    payload BLOB,
    encoding TEXT,
    timestamp BLOB
) WITHOUT ROWID;
"#;

// Properties used by the Backend
//  - None

// Properties used by the Storage
const PROP_STORAGE_DIR: &str = "dir";
const PROP_STORAGE_READ_ONLY: &str = "read_only";
const PROP_STORAGE_ON_CLOSURE: &str = "on_closure";

// Special key for None (when the prefix being stripped exactly matches the key)
const NONE_KEY: &str = "@@none_key@@";

enum OnClosure {
    DestroyDB,
    DoNothing,
}

struct SqliteBackend;
zenoh_plugin_trait::declare_plugin!(SqliteBackend);

impl Plugin for SqliteBackend {
    type StartArgs = VolumeConfig;
    type Instance = VolumeInstance;

    const DEFAULT_NAME: &'static str = "sqlite_backend";
    const PLUGIN_VERSION: &'static str = plugin_version!();
    const PLUGIN_LONG_VERSION: &'static str = plugin_long_version!();

    fn start(_name: &str, _args: &VolumeConfig) -> ZResult<VolumeInstance> {
        let root_dir = if let Some(dir) = std::env::var_os(SCOPE_ENV_VAR) {
            PathBuf::from(dir)
        } else {
            let mut dir = PathBuf::from(zenoh_home());
            dir.push(DEFAULT_ROOT_DIR);
            dir
        };
        Ok(Box::new(SqliteVolume { root_dir }))
    }
}

struct SqliteVolume {
    root_dir: PathBuf,
}

#[async_trait]
impl Volume for SqliteVolume {
    /// Returns the status that will be sent as a reply to a query
    /// on the administration space for this backend.
    fn get_admin_status(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert("root".into(), self.root_dir.to_string_lossy().into());
        map.insert("version".into(), SqliteBackend::PLUGIN_VERSION.into());
        map.into()
    }

    /// Returns the capability of this backend
    fn get_capability(&self) -> Capability {
        Capability {
            persistence: Persistence::Durable,
            history: History::Latest,
            read_cost: 0,
        }
    }

    /// Creates a storage configured with some properties.
    async fn create_storage(&self, props: StorageConfig) -> ZResult<Box<dyn Storage>> {
        let volume_cfg = match props.volume_cfg.as_object() {
            Some(v) => v,
            None => Err("sqlite backed storages need volume-specific configurations")?,
        };

        let read_only = match volume_cfg.get(PROP_STORAGE_READ_ONLY) {
            None | Some(serde_json::Value::Bool(false)) => false,
            Some(serde_json::Value::Bool(true)) => true,
            _ => Err(format!(
                "Optional property `{}` of sqlite storage configurations must be a boolean",
                PROP_STORAGE_READ_ONLY
            ))?,
        };

        let on_closure = match volume_cfg.get(PROP_STORAGE_ON_CLOSURE) {
            Some(serde_json::Value::String(s)) if s == "destroy_db" => OnClosure::DestroyDB,
            Some(serde_json::Value::String(s)) if s == "do_nothing" => OnClosure::DoNothing,
            None => OnClosure::DoNothing,
            _ => Err(format!(
                r#"Optional property `{}` of sqlite storage configurations must be either "do_nothing" (default) or "destroy_db""#,
                PROP_STORAGE_ON_CLOSURE
            ))?,
        };

        let mut db_path = match volume_cfg.get(PROP_STORAGE_DIR) {
            Some(serde_json::Value::String(dir)) => {
                let mut db_path = self.root_dir.clone();
                db_path.push(dir);
                db_path
            }
            _ => Err(format!(
                r#"Required property `{}` for File System Storage must be a string"#,
                PROP_STORAGE_DIR
            ))?,
        };

        debug!("Creating directory for sqlite storage at: {:?}", db_path);
        fs::create_dir_all(&db_path)?;

        // Append the file name to the path
        db_path.push(DEFAULT_FILE_NAME);

        debug!("Creating sqlite storage at: {:?}", db_path);
        let storage = SqliteStorage::new(&db_path, props.to_json_value(), read_only, on_closure)?;
        Ok(Box::new(storage))
    }

    /// Returns an interceptor that will be called before pushing any data
    /// into a storage created by this backend. `None` can be returned for no interception point.
    fn incoming_data_interceptor(&self) -> Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>> {
        None
    }

    /// Returns an interceptor that will be called before sending any reply
    /// to a query from a storage created by this backend. `None` can be returned for no interception point.
    fn outgoing_data_interceptor(&self) -> Option<Arc<dyn Fn(Sample) -> Sample + Send + Sync>> {
        None
    }
}

struct SqliteStorage {
    admin_status: serde_json::Value,
    read_only: bool,
    on_closure: OnClosure,
    db: Arc<Mutex<Option<Connection>>>,
}

impl SqliteStorage {
    pub(crate) fn new(
        db_path: &Path,
        admin_status: serde_json::Value,
        read_only: bool,
        on_closure: OnClosure,
    ) -> ZResult<Self> {
        let mut connection = Connection::open(db_path)?;

        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "synchronous", "FULL")?;

        trace!("Creating table if not exists");
        let tx = connection.transaction()?;
        tx.execute(CREATE_TABLE_STMT, ())?;
        tx.commit()?;

        let db = Arc::new(Mutex::new(Some(connection)));
        Ok(Self {
            admin_status,
            read_only,
            on_closure,
            db,
        })
    }
}

impl Drop for SqliteStorage {
    fn drop(&mut self) {
        if let Ok(mut db) = self.db.lock() {
            if let Some(conn) = db.take() {
                // Save the path before closing the connection
                let path = conn.path().unwrap_or("").to_owned();
                if let Err(e) = conn.close() {
                    error!("Error closing connection: {:?}", e);
                }
                if matches!(self.on_closure, OnClosure::DestroyDB) {
                    if path.is_empty() {
                        error!("No path for db file, unable to remove it");
                        return;
                    }
                    debug!("Removing the db file: {:?}", path);
                    if let Err(e) = fs::remove_file(path) {
                        error!("Error removing db file: {:?}", e);
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Storage for SqliteStorage {
    /// Returns the status that will be sent as a reply to a query
    /// on the administration space for this storage.
    fn get_admin_status(&self) -> serde_json::Value {
        self.admin_status.clone()
    }

    /// Function called for each incoming data ([`Sample`]) to be stored in this storage.
    /// A key can be `None` if it matches the `strip_prefix` exactly.
    /// In order to avoid data loss, the storage must store the `value` and `timestamp` associated with the `None` key
    /// in a manner suitable for the given backend technology
    async fn put(
        &mut self,
        key: Option<OwnedKeyExpr>,
        value: Value,
        timestamp: Timestamp,
    ) -> ZResult<StorageInsertionResult> {
        let mut db_cell = self.db.lock().unwrap();
        let db = db_cell.as_mut().unwrap();

        let key = match key {
            Some(k) => k.to_string(),
            None => NONE_KEY.to_owned(),
        };

        trace!("Putting key {:?} with timestamp {:?}", key, timestamp);

        if !self.read_only {
            match set_value(db, &key, value, timestamp) {
                Ok(_) => Ok(StorageInsertionResult::Inserted),
                Err(e) => Err(format!("Error when inserting key {:?} : {}", key, e).into()),
            }
        } else {
            error!("Received PUT for read-only DB on {:?} - ignored", key);
            Err("Received update for read-only DB".into())
        }
    }

    /// Function called for each incoming delete request to this storage.
    /// A key can be `None` if it matches the `strip_prefix` exactly.
    /// In order to avoid data loss, the storage must delete the entry corresponding to the `None` key
    /// in a manner suitable for the given backend technology
    async fn delete(
        &mut self,
        key: Option<OwnedKeyExpr>,
        timestamp: Timestamp,
    ) -> ZResult<StorageInsertionResult> {
        let mut db_cell = self.db.lock().unwrap();
        let db = db_cell.as_mut().unwrap();

        let key = match key {
            Some(k) => k.to_string(),
            None => NONE_KEY.to_owned(),
        };

        trace!("Deleting key {:?} with timestamp {:?}", key, timestamp);

        if !self.read_only {
            match delete_value(db, &key) {
                Ok(_) => Ok(StorageInsertionResult::Deleted),
                Err(e) => Err(format!("Error when deleting key {:?} : {}", key, e).into()),
            }
        } else {
            error!("Received DELETE for read-only DB on {:?} - ignored", key);
            Err("Received update for read-only DB".into())
        }
    }

    /// Function to retrieve the sample associated with a single key.
    /// A key can be `None` if it matches the `strip_prefix` exactly.
    /// In order to avoid data loss, the storage must retrieve the `value` and `timestamp` associated with the `None` key
    /// in a manner suitable for the given backend technology
    async fn get(
        &mut self,
        key: Option<OwnedKeyExpr>,
        parameters: &str,
    ) -> ZResult<Vec<StoredData>> {
        let db_cell = self.db.lock().unwrap();
        let db = db_cell.as_ref().unwrap();

        let key = match key {
            Some(k) => k.to_string(),
            None => NONE_KEY.to_owned(),
        };

        trace!("Getting key {:?} with parameters {:?}", key, parameters);

        match get_value(db, &key) {
            Ok(Some(data)) => Ok(vec![data]),
            Ok(None) => Ok(vec![]),
            Err(e) => Err(format!("Error when getting key {:?} : {}", key, e).into()),
        }
    }

    /// Function called to get the list of all storage content (key, timestamp)
    /// The latest Timestamp corresponding to each key is either the timestamp of the delete or put whichever is the latest.
    /// Remember to fetch the entry corresponding to the `None` key
    async fn get_all_entries(&self) -> ZResult<Vec<(Option<OwnedKeyExpr>, Timestamp)>> {
        let db_cell = self.db.lock().unwrap();
        let db = db_cell.as_ref().unwrap();

        let mut stmt = db.prepare("SELECT key, timestamp FROM zenoh_storage")?;
        let iter = stmt.query_map([], |row| {
            let key: String = row.get(0)?;
            let timestamp: Vec<u8> = row.get(1)?;
            Ok((key, timestamp))
        })?;

        let mut result = Vec::new();
        for item in iter {
            let (k, t) = item?;
            trace!("Found item, key: {:?}", k);
            let key = if k == NONE_KEY {
                None
            } else {
                Some(OwnedKeyExpr::new(k)?)
            };
            let timestamp = decode_timestamp(&t)?;
            result.push((key, timestamp));
        }
        Ok(result)
    }
}

fn set_value(db: &mut Connection, key: &str, value: Value, timestamp: Timestamp) -> ZResult<()> {
    let payload = value.payload.contiguous();
    let encoding = value.encoding.to_string();
    let timestamp = encode_timestamp(timestamp)?;
    let tx = db.transaction()?;
    tx.execute(
        "INSERT OR REPLACE INTO zenoh_storage (key, payload, encoding, timestamp) VALUES (?, ?, ?, ?)",
        params![key, &payload, &encoding, &timestamp],
    )?;
    tx.commit()?;
    Ok(())
}

fn delete_value(db: &mut Connection, key: &str) -> ZResult<()> {
    let tx = db.transaction()?;
    tx.execute("DELETE FROM zenoh_storage WHERE key = ?", [key])?;
    tx.commit()?;
    Ok(())
}

fn get_value(db: &Connection, key: &str) -> ZResult<Option<StoredData>> {
    let mut stmt =
        db.prepare("SELECT payload, encoding, timestamp FROM zenoh_storage WHERE key = ?")?;
    let mut rows = stmt.query([key])?;
    if let Some(row) = rows.next()? {
        let payload: Vec<u8> = row.get(0)?;
        let encoding: String = row.get(1)?;
        let timestamp: Vec<u8> = row.get(2)?;
        let value = Value::new(payload.into()).encoding(encoding.into());
        let timestamp = decode_timestamp(&timestamp)?;
        Ok(Some(StoredData { value, timestamp }))
    } else {
        Ok(None)
    }
}

fn decode_timestamp(buf: &[u8]) -> ZResult<Timestamp> {
    let codec = Zenoh080::new();
    let mut reader = buf.reader();
    let timestamp: Timestamp = codec
        .read(&mut reader)
        .map_err(|_| "Failed to decode timestamp")?;
    Ok(timestamp)
}

fn encode_timestamp(timestamp: Timestamp) -> ZResult<Vec<u8>> {
    let codec = Zenoh080::new();
    let mut out = Vec::with_capacity(16);
    let mut writer = out.writer();
    codec
        .write(&mut writer, &timestamp)
        .map_err(|_| "Failed to encode timestamp")?;
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use uhlc::ID;

    #[test]
    fn it_works() {
        let mut storage = SqliteStorage::new(
            Path::new(":memory:"),
            serde_json::Value::Null,
            false,
            OnClosure::DoNothing,
        )
        .expect("failed to create storage");

        // Check that the table is empty
        let entries = smol::block_on(storage.get_all_entries()).expect("failed to get all entries");
        assert_eq!(entries.is_empty(), true);

        // Test put
        let key = OwnedKeyExpr::new("foo/bar").expect("failed to create key");
        let value = Value::new(vec![1, 2, 3].into()).encoding("raw".into());
        let id: ID = ID::try_from([0x02]).unwrap();
        let timestamp = Timestamp::new(Default::default(), id);
        smol::block_on(storage.put(Some(key.clone()), value, timestamp)).expect("failed to put");

        // Check that put succeeded
        let entries = smol::block_on(storage.get_all_entries()).expect("failed to get all entries");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0.as_ref().unwrap().to_string(), "foo/bar");
        assert_eq!(entries[0].1, timestamp);

        // Test get
        let data = smol::block_on(storage.get(Some(key.clone()), "")).expect("failed to get");
        assert_eq!(data.len(), 1);
        assert_eq!(data[0].value.payload.contiguous(), vec![1, 2, 3]);
        assert_eq!(data[0].value.encoding.to_string(), "raw");
        assert_eq!(data[0].timestamp, timestamp);

        // Test delete
        smol::block_on(storage.delete(Some(key.clone()), timestamp)).expect("failed to delete");

        // Check that delete succeeded
        let entries = smol::block_on(storage.get_all_entries()).expect("failed to get all entries");
        assert_eq!(entries.is_empty(), true);
    }
}
