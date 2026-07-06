use failure::Error;
use std::{collections::HashMap, path::Path};

pub struct KvStore {
    scores: HashMap<String, String>,
}

pub type Result<T> = std::result::Result<T, Error>;

impl KvStore {
    pub fn open(temp_file: &Path) -> Result<KvStore> {
        // KvStore {
        //     scores: HashMap::new(),
        // }
        unimplemented!()
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        // self.scores.get(&key).cloned()
        unimplemented!()
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        // self.scores.insert(key, value);
        unimplemented!()
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        // self.scores.remove(&key);
        unimplemented!()
    }
}
