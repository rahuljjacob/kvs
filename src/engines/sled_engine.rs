use crate::server::KvsEngine;
pub use crate::shared::Result;
use sled::Db;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

pub struct SharedSledKvsEngine {
    pub store: Db,
}

#[derive(Clone)]
pub struct SledKvsEngine(Arc<Mutex<SharedSledKvsEngine>>);

impl KvsEngine for SledKvsEngine {
    fn get(&self, key: String) -> Result<Option<String>> {
        let sled_store = self.0.lock().unwrap();
        let result = sled_store.store.get(key)?;
        match result {
            Some(value) => Ok(Some(String::from_utf8(value.to_vec())?)),
            None => Ok(None),
        }
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        let sled_store = self.0.lock().unwrap();
        sled_store.store.insert(key, value.as_bytes())?;
        sled_store.store.flush()?;
        Ok(())
    }

    fn remove(&self, key: String) -> Result<()> {
        let sled_store = self.0.lock().unwrap();
        let result = sled_store.store.remove(key)?;
        sled_store.store.flush()?;
        match result {
            Some(_) => Ok(()),
            None => return Err(failure::format_err!("Key not found")),
        }
    }
}

impl SledKvsEngine {
    pub fn open(path: impl Into<PathBuf>) -> Result<SledKvsEngine> {
        let path: PathBuf = path.into();
        let db = sled::open(path)?;
        Ok(SledKvsEngine(Arc::new(Mutex::new(SharedSledKvsEngine {
            store: db,
        }))))
    }
}
