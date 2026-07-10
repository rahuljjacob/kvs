use failure::Error;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Write},
    path::PathBuf,
};

pub struct KvStore {
    store: HashMap<String, String>,
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
enum WriteCommand {
    Set(Set),
    Rm(Rm),
}

pub type Result<T> = std::result::Result<T, Error>;

impl KvStore {
    pub fn open(temp_path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = temp_path.into();

        let path = path.join("db.txt");
        File::options().append(true).create(true).open(&path)?;
        Ok(KvStore {
            store: HashMap::new(),
            path: path,
        })
    }

    // Replay log, create in memory index for KV
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let file = File::open(&self.path)?;
        let reader = BufReader::new(file);
        let kv_stream = serde_json::Deserializer::from_reader(reader).into_iter::<WriteCommand>();
        for command in kv_stream {
            let command = command?;
            match command {
                WriteCommand::Set(Set { key, value }) => self.store.insert(key, value),
                WriteCommand::Rm(Rm { key }) => self.store.remove(&key),
            };
        }

        let val = self.store.get(&key);
        Ok(val.cloned())
    }

    // Serialize key and value using Serde and write to file
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let set_op = WriteCommand::Set(Set { key, value });
        let serialized = serde_json::to_string(&set_op)?;
        let mut file = File::options().append(true).create(true).open(&self.path)?;
        file.write_all(serialized.as_bytes())?;
        Ok(())
    }

    // Serialize key and write
    pub fn remove(&mut self, key: String) -> Result<()> {
        let exists = self.get(key.clone());
        let exists = exists?;

        if exists.is_none() {
            return Err(failure::format_err!("No key gng"));
        }

        let del_op = WriteCommand::Rm(Rm { key });
        let serialized = serde_json::to_string(&del_op)?;
        let mut file = File::options().append(true).create(true).open(&self.path)?;
        file.write_all(serialized.as_bytes())?;
        Ok(())
    }
}
