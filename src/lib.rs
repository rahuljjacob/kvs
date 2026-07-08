use failure::Error;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::Write,
    path::PathBuf,
};

pub struct KvStore {
    scores: HashMap<String, String>,
    path: PathBuf,
}

#[derive(Serialize, Deserialize)]
pub struct Set {
    key: String,
    value: String,
}

#[derive(Serialize, Deserialize)]
pub struct Rm {
    key: String,
}

#[derive(Serialize, Deserialize)]
enum WriteCommands {
    Set(Set),
    Rm(Rm),
}

pub type Result<T> = std::result::Result<T, Error>;

impl KvStore {
    pub fn open(temp_path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = temp_path.into();

        let path = path.join("db.txt");
        Ok(KvStore {
            scores: HashMap::new(),
            path: path,
        })
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        unimplemented!()
    }

    // Serialize key and value using Serde and write to file
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let set_op = WriteCommands::Set(Set { key, value });
        let serialized = serde_json::to_string(&set_op)?;
        let mut file = File::options().append(true).create(true).open(&self.path)?;
        file.write_all(serialized.as_bytes())?;
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        unimplemented!()
    }
}
