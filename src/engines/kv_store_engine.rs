use crate::server::KvsEngine;
pub use crate::shared::{Result, Rm, Set, WriteCommand};
use std::sync::{Arc, Mutex};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Seek, SeekFrom, Write},
    path::PathBuf,
};

const THRESHOLD: u64 = 1 * 1024 * 1024;

pub struct SharedKvStore {
    store: HashMap<String, (u64, u64)>,
    path: PathBuf,
    uncompacted: u64,
}

#[derive(Clone)]
pub struct KvStore(Arc<Mutex<SharedKvStore>>);

impl KvsEngine for KvStore {
    fn get(&self, key: String) -> Result<Option<String>> {
        let kv_store = self.0.lock().unwrap();
        let mut file = File::open(&kv_store.path)?;
        let Some(offset) = kv_store.store.get(&key).copied() else {
            return Ok(None);
        };

        file.seek(SeekFrom::Start(offset.0))?;
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
    fn set(&self, key: String, value: String) -> Result<()> {
        let mut kv_store = self.0.lock().unwrap();
        let mut file = File::options()
            .append(true)
            .create(true)
            .open(&kv_store.path)?;
        let offset = file.seek(SeekFrom::End(0))?;

        let set_op = WriteCommand::Set(Set {
            key: key.clone(),
            value,
        });
        let serialized = serde_json::to_string(&set_op)?;
        file.write_all(serialized.as_bytes())?;
        let end_offset = file.stream_position()?;
        let len = end_offset - offset;

        if let Some(old_value) = kv_store.store.get(&key) {
            kv_store.uncompacted = kv_store.uncompacted + old_value.1;
        }
        kv_store.store.insert(key, (offset, len));

        if kv_store.uncompacted > THRESHOLD {
            kv_store.compact()?;
        }

        Ok(())
    }

    // Serialize key and write
    fn remove(&self, key: String) -> Result<()> {
        let mut kv_store = self.0.lock().unwrap();
        if let Some(old_value) = kv_store.store.get(&key) {
            kv_store.uncompacted = kv_store.uncompacted + old_value.1
        } else {
            return Err(failure::format_err!("Key not found"));
        }

        kv_store.store.remove(&key);
        let del_op = WriteCommand::Rm(Rm { key });
        let serialized = serde_json::to_string(&del_op)?;
        let mut file = File::options()
            .append(true)
            .create(true)
            .open(&kv_store.path)?;
        file.write_all(serialized.as_bytes())?;

        if kv_store.uncompacted > THRESHOLD {
            kv_store.compact()?;
        }

        Ok(())
    }
}

impl SharedKvStore {
    pub fn compact(&mut self) -> Result<()> {
        let path = self.path.clone();
        let path = path.with_file_name("db2.txt");
        let mut new_store: HashMap<String, (u64, u64)> = HashMap::new();

        let mut file = File::options()
            .append(true)
            .read(true)
            .create(true)
            .open(&path)?;

        let mut reader = BufReader::new(File::open(&self.path)?);

        for (key, _) in self.store.clone() {
            let offset = self.store.get(&key).copied().unwrap();
            reader.seek(SeekFrom::Start(offset.0))?;

            let mut kv_stream =
                serde_json::Deserializer::from_reader(&mut reader).into_iter::<WriteCommand>();

            let value = match kv_stream.next().unwrap()? {
                WriteCommand::Set(set) => set.value,
                WriteCommand::Rm(_) => unreachable!(),
            };

            let new_offset = file.stream_position()?;
            let set_op = WriteCommand::Set(Set {
                key: key.clone(),
                value,
            });
            let serialized = serde_json::to_string(&set_op)?;
            file.write_all(serialized.as_bytes())?;
            let end_offset = file.stream_position()?;

            new_store.insert(key, (new_offset, end_offset - new_offset));
        }

        std::fs::rename(&path, &self.path)?;
        self.store = new_store;
        self.uncompacted = 0;

        Ok(())
    }
}

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

        let mut kv_store = SharedKvStore {
            store: HashMap::new(),
            path: path,
            uncompacted: 0,
        };

        let mut pos = 0;

        while let Some(command) = kv_stream.next() {
            let end_pos = kv_stream.byte_offset() as u64;
            let command = command?;
            match command {
                WriteCommand::Set(Set { key, value: _value }) => {
                    kv_store.store.insert(key, (pos, end_pos - pos))
                }
                WriteCommand::Rm(Rm { key }) => kv_store.store.remove(&key),
            };
            pos = end_pos;
        }

        Ok(KvStore(Arc::new(Mutex::new(kv_store))))
    }
}
