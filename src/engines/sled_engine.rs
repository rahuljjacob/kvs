use crate::server::KvsEngine;
pub use crate::shared::Result;
use sled::Db;
use std::path::PathBuf;

#[derive(Clone)]
pub struct SledKvsEngine(Db);

impl KvsEngine for SledKvsEngine {
    fn get(&self, key: String) -> Result<Option<String>> {
        let result = self.0.get(key)?;
        match result {
            Some(value) => Ok(Some(String::from_utf8(value.to_vec())?)),
            None => Ok(None),
        }
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        self.0.insert(key, value.as_bytes())?;
        self.0.flush()?;
        Ok(())
    }

    fn remove(&self, key: String) -> Result<()> {
        let result = self.0.remove(key)?;
        self.0.flush()?;
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
        Ok(SledKvsEngine(db))
    }
}
