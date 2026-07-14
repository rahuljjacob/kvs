use failure::Error;
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Seek, SeekFrom, Write},
    path::PathBuf,
};

pub struct KvStore {
    store: HashMap<String, u64>,
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
        let file = File::options()
            .append(true)
            .read(true)
            .create(true)
            .open(&path)?;

        // Replay log into KvStore
        let reader = BufReader::new(file);
        let mut kv_stream =
            serde_json::Deserializer::from_reader(reader).into_iter::<WriteCommand>();

        let mut kv_store = KvStore {
            store: HashMap::new(),
            path: path,
        };

        let mut pos = 0;

        while let Some(command) = kv_stream.next() {
            let end_pos = kv_stream.byte_offset() as u64;
            let command = command?;
            match command {
                WriteCommand::Set(Set { key, value }) => kv_store.store.insert(key, pos),
                WriteCommand::Rm(Rm { key }) => kv_store.store.remove(&key),
            };
            pos = end_pos;
        }

        Ok(kv_store)
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let mut file = File::open(&self.path)?;
        let Some(offset) = self.store.get(&key).copied() else {
            return Ok(None);
        };

        file.seek(SeekFrom::Start(offset))?;
        let reader = BufReader::new(file);
        let mut kv_stream =
            serde_json::Deserializer::from_reader(reader).into_iter::<WriteCommand>();

        let value = kv_stream.next().unwrap()?;

        match value {
            WriteCommand::Set(set) => Ok(Some(set.value)),
            WriteCommand::Rm(_rm) => unreachable!(),
        }
    }

    // Serialize key and value using Serde and write to file
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let mut file = File::options().append(true).create(true).open(&self.path)?;
        let offset = file.seek(SeekFrom::End(0))?;

        let set_op = WriteCommand::Set(Set {
            key: key.clone(),
            value,
        });
        let serialized = serde_json::to_string(&set_op)?;
        file.write_all(serialized.as_bytes())?;

        self.store.insert(key, offset);
        Ok(())
    }

    // Serialize key and write
    pub fn remove(&mut self, key: String) -> Result<()> {
        let exists = self.store.get(&key);

        if exists.is_none() {
            return Err(failure::format_err!("Key not found"));
        }

        self.store.remove(&key);
        let del_op = WriteCommand::Rm(Rm { key });
        let serialized = serde_json::to_string(&del_op)?;
        let mut file = File::options().append(true).create(true).open(&self.path)?;
        file.write_all(serialized.as_bytes())?;

        Ok(())
    }
}
