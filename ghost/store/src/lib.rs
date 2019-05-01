use std::io;
use std::sync::Arc;

use kvdb::{DBOp, DBTransaction, KeyValueDB};
use serde::de::DeserializeOwned;

use primitives::serialize::{Decode, Encode};

pub mod test_utils;

pub const COL_BLOCK_MISC: Option<u32> = Some(0);
pub const COL_BLOCK: Option<u32> = Some(1);
pub const COL_BLOCK_HEADER: Option<u32> = Some(2);
pub const COL_STATE: Option<u32> = Some(3);
pub const COL_STATE_REF: Option<u32> = Some(4);
pub const COL_PEERS: Option<u32> = Some(5);
const NUM_COLS: u32 = 5;

pub struct Store {
    storage: Arc<KeyValueDB>,
}

impl Store {
    pub fn new(storage: Arc<KeyValueDB>) -> Store {
        Store { storage }
    }

    pub fn get_ser<T: Decode + DeserializeOwned + std::fmt::Debug>(
        &self,
        column: Option<u32>,
        key: &[u8],
    ) -> Result<Option<T>, io::Error> {
        match self.storage.get(column, key) {
            Ok(Some(bytes)) => match Decode::decode(bytes.as_ref()) {
                Ok(result) => Ok(Some(result)),
                Err(e) => Err(e),
            },
            Ok(None) => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn exists(&self, column: Option<u32>, key: &[u8]) -> Result<bool, io::Error> {
        match self.storage.get(column, key) {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(e),
        }
    }

    pub fn store_update(&self) -> StoreUpdate {
        StoreUpdate::new(self.storage.clone())
    }
}

/// Keeps track of current changes to the database and can commit all of them to the database.
pub struct StoreUpdate {
    storage: Arc<KeyValueDB>,
    transaction: DBTransaction,
}

impl StoreUpdate {
    pub fn new(storage: Arc<KeyValueDB>) -> Self {
        let transaction = storage.transaction();
        StoreUpdate { storage, transaction }
    }

    pub fn set(&mut self, column: Option<u32>, key: &[u8], value: &[u8]) {
        self.transaction.put(column, key, value)
    }

    pub fn set_ser<T: Encode>(
        &mut self,
        column: Option<u32>,
        key: &[u8],
        value: &T,
    ) -> Result<(), io::Error> {
        let data = Encode::encode(value)?;
        self.set(column, key, &data);
        Ok(())
    }

    pub fn delete(&mut self, column: Option<u32>, key: &[u8]) {
        self.transaction.delete(column, key);
    }

    /// Merge another store update into this one.
    pub fn merge(&mut self, other: StoreUpdate) {
        for op in other.transaction.ops {
            match op {
                DBOp::Insert { col, key, value } => self.transaction.put(col, &key, &value),
                DBOp::Delete { col, key } => self.transaction.delete(col, &key),
            }
        }
    }

    pub fn commit(self) -> Result<(), io::Error> {
        self.storage.write(self.transaction)
    }
}