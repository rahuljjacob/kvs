use failure::Error;
use std::{collections::HashMap, fs::File, path::PathBuf};

pub struct KvStore {
    scores: HashMap<String, String>,
    path: PathBuf,
}

pub type Result<T> = std::result::Result<T, Error>;

impl KvStore {
    pub fn open(temp_path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = temp_path.into();

        if path.exists() == false {
            Err(failure::format_err!("Path does not exist"))
        } else {
            let path = path.join("db.txt");
            File::create(&path)?;
            Ok(KvStore {
                scores: HashMap::new(),
                path: path,
            })
        }
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
