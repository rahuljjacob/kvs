use crossbeam_skiplist::SkipMap;

use crate::server::KvsEngine;
pub use crate::shared::{Result, Rm, Set, WriteCommand};
use std::sync::{Arc, Mutex, RwLock};
use std::{
    fs::File,
    io::{BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const THRESHOLD: u64 = 1 * 1024 * 1024;

pub struct SharedKvStore {
    uncompacted: u64,
    writer: BufWriterWithPos,
    current_generation: u64,
}

#[derive(Clone)]
pub struct KvStore {
    store: Arc<Mutex<SharedKvStore>>,
    index: Arc<SkipMap<String, CommandPos>>,
    compaction_lock: Arc<RwLock<()>>,
    path: Arc<PathBuf>,
}

pub struct BufWriterWithPos {
    writer_fd: File,
    //Offset to end of file
    offset: u64,
}

#[derive(Clone)]
pub struct CommandPos {
    offset: u64,
    len: u64,
    generation: u64,
}

impl KvsEngine for KvStore {
    fn get(&self, key: String) -> Result<Option<String>> {
        // Read lock is dropped when _guard is dropped
        let _guard = self.compaction_lock.read().unwrap();
        let Some(command) = self.index.get(&key) else {
            return Ok(None);
        };

        let mut file = File::open(log_path(&self.path, command.value().generation))?;
        file.seek(SeekFrom::Start(command.value().offset))?;
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
        let mut kv_store = self.store.lock().unwrap();

        let set_op = WriteCommand::Set(Set {
            key: key.clone(),
            value,
        });
        let serialized = serde_json::to_string(&set_op)?;
        let offset = kv_store.writer.offset;
        kv_store.writer.writer_fd.write_all(serialized.as_bytes())?;
        let len = serialized.len() as u64;
        kv_store.writer.offset = offset + len;

        if let Some(old_value) = self.index.get(&key) {
            kv_store.uncompacted = kv_store.uncompacted + old_value.value().len;
        }
        self.index.insert(
            key,
            CommandPos {
                offset,
                len,
                generation: kv_store.current_generation,
            },
        );

        if kv_store.uncompacted > THRESHOLD {
            let _guard = self.compaction_lock.write().unwrap();
            kv_store.compact(self.index.clone(), self.path.clone())?;
        }

        Ok(())
    }

    // Serialize key and write
    fn remove(&self, key: String) -> Result<()> {
        let mut kv_store = self.store.lock().unwrap();
        if let Some(old_value) = self.index.get(&key) {
            kv_store.uncompacted = kv_store.uncompacted + old_value.value().len
        } else {
            return Err(failure::format_err!("Key not found"));
        }

        let del_op = WriteCommand::Rm(Rm { key: key.clone() });
        let serialized = serde_json::to_string(&del_op)?;
        let len = serialized.len() as u64;
        kv_store.writer.writer_fd.write_all(serialized.as_bytes())?;
        kv_store.writer.offset = kv_store.writer.offset + len;
        self.index.remove(&key);

        if kv_store.uncompacted > THRESHOLD {
            let _guard = self.compaction_lock.write().unwrap();
            kv_store.compact(self.index.clone(), self.path.clone())?;
        }

        Ok(())
    }
}

impl SharedKvStore {
    pub fn compact(
        &mut self,
        index: Arc<SkipMap<String, CommandPos>>,
        path: Arc<PathBuf>,
    ) -> Result<()> {
        let new_path = path.join("compact.tmp");
        let new_index = SkipMap::new();

        let mut file = File::options()
            .append(true)
            .read(true)
            .create(true)
            .open(&new_path)?;

        let mut reader = BufReader::new(File::open(log_path(&path, 0))?);

        for entry in index.iter() {
            let key = entry.key();
            let command = entry.value();
            reader.seek(SeekFrom::Start(command.offset))?;

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

            new_index.insert(key.clone(), (new_offset, end_offset - new_offset));
        }

        std::fs::rename(&new_path, log_path(&path, 0))?;
        for (key, (offset, len)) in new_index {
            index.insert(
                key,
                CommandPos {
                    offset,
                    len,
                    generation: 0,
                },
            );
        }
        self.writer.writer_fd = File::options()
            .append(true)
            .create(true)
            .open(log_path(&path, 0))?;
        self.writer.offset = file.stream_position()?;
        self.uncompacted = 0;

        Ok(())
    }
}

impl BufWriterWithPos {
    pub fn new(file: File, offset: u64) -> BufWriterWithPos {
        BufWriterWithPos {
            writer_fd: file,
            offset,
        }
    }
}

impl KvStore {
    pub fn open(temp_path: impl Into<PathBuf>) -> Result<KvStore> {
        let path: PathBuf = temp_path.into();
        let file = File::options()
            .append(true)
            .read(true)
            .create(true)
            .open(log_path(&path, 0))?;
        let index = SkipMap::new();

        // Replay log into KvStore
        let reader = BufReader::new(file);
        let mut kv_stream =
            serde_json::Deserializer::from_reader(reader).into_iter::<WriteCommand>();

        let writer_file = File::options()
            .append(true)
            .create(true)
            .open(log_path(&path, 0))?;

        let mut pos = 0;

        while let Some(command) = kv_stream.next() {
            let command = match command {
                Ok(command) => command,
                Err(e) if e.is_eof() => break,
                Err(e) => return Err(e.into()),
            };
            let end_pos = kv_stream.byte_offset() as u64;
            match command {
                WriteCommand::Set(Set { key, value: _value }) => {
                    index.insert(
                        key,
                        CommandPos {
                            offset: pos,
                            len: end_pos - pos,
                            generation: 0,
                        },
                    );
                }
                WriteCommand::Rm(Rm { key }) => {
                    index.remove(&key);
                }
            };
            pos = end_pos;
        }

        // Truncate tail if corrupted
        writer_file.set_len(pos)?;

        let kv_store = SharedKvStore {
            writer: BufWriterWithPos::new(writer_file, pos),
            uncompacted: 0,
            current_generation: 0,
        };

        Ok(KvStore {
            index: Arc::new(index),
            store: Arc::new(Mutex::new(kv_store)),
            compaction_lock: Arc::new(RwLock::new(())),
            path: Arc::new(path),
        })
    }
}

fn log_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("{}.log", generation))
}
