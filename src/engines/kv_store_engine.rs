use crate::server::KvsEngine;
pub use crate::shared::{Result, Rm, Set, WriteCommand};
use crossbeam_skiplist::SkipMap;
use std::io::ErrorKind;
use std::sync::{Arc, Mutex};
use std::{
    fs::File,
    io::{BufReader, Seek, SeekFrom, Write},
    path::{Path, PathBuf},
};

const THRESHOLD: u64 = 1 * 1024 * 1024;

pub struct SharedKvStore {
    fragmentation: u64,
    writer: BufWriterWithPos,
    current_generation: u64,
}

#[derive(Clone)]
pub struct KvStore {
    store: Arc<Mutex<SharedKvStore>>,
    index: Arc<SkipMap<String, CommandPos>>,
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
        let Some(command) = self.index.get(&key) else {
            return Ok(None);
        };

        let mut pos = command.value().clone();
        let mut file = match File::open(log_path(&self.path, command.value().generation)) {
            Ok(file) => file,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                let Some(command) = self.index.get(&key) else {
                    return Ok(None);
                };
                pos = command.value().clone();
                File::open(log_path(&self.path, pos.generation))?
            }
            Err(e) => return Err(e.into()),
        };
        file.seek(SeekFrom::Start(pos.offset))?;
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
            kv_store.fragmentation = kv_store.fragmentation + old_value.value().len;
        }
        self.index.insert(
            key,
            CommandPos {
                offset,
                len,
                generation: kv_store.current_generation,
            },
        );

        if kv_store.fragmentation > THRESHOLD {
            kv_store.compact(self.index.clone(), self.path.clone())?;
        }

        Ok(())
    }

    // Serialize key and write
    fn remove(&self, key: String) -> Result<()> {
        let mut kv_store = self.store.lock().unwrap();
        if let Some(old_value) = self.index.get(&key) {
            kv_store.fragmentation = kv_store.fragmentation + old_value.value().len
        } else {
            return Err(failure::format_err!("Key not found"));
        }

        let del_op = WriteCommand::Rm(Rm { key: key.clone() });
        let serialized = serde_json::to_string(&del_op)?;
        let len = serialized.len() as u64;
        kv_store.writer.writer_fd.write_all(serialized.as_bytes())?;
        kv_store.writer.offset = kv_store.writer.offset + len;
        self.index.remove(&key);

        if kv_store.fragmentation > THRESHOLD {
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
        let compacted_file_generation = self.current_generation + 1;

        let mut file = File::options()
            .append(true)
            .create(true)
            .open(&log_path(&path, compacted_file_generation))?;

        for entry in index.iter() {
            let key = entry.key();
            let command = entry.value();

            let mut reader = BufReader::new(File::open(log_path(&path, command.generation))?);
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

            index.insert(
                key.clone(),
                CommandPos {
                    offset: new_offset,
                    len: end_offset - new_offset,
                    generation: compacted_file_generation,
                },
            );
        }

        self.writer.writer_fd = File::options()
            .append(true)
            .create(true)
            .open(log_path(&path, compacted_file_generation + 1))?;
        self.writer.offset = 0;
        self.fragmentation = 0;
        self.current_generation = compacted_file_generation + 1;

        remove_generations_below(&path, compacted_file_generation)?;

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
        let index = SkipMap::new();

        let generation_list = get_generations(&path)?;

        let max_generation = match generation_list.last() {
            Some(generation) => generation.clone() + 1,
            None => 0 as u64,
        };

        let writer_file = File::options()
            .append(true)
            .create(true)
            .open(log_path(&path, max_generation))?;

        for generation in generation_list {
            let mut pos = 0;
            let file = File::options()
                .append(true)
                .read(true)
                .create(true)
                .open(log_path(&path, generation))?;

            let reader = BufReader::new(file);
            let mut kv_stream =
                serde_json::Deserializer::from_reader(reader).into_iter::<WriteCommand>();

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
                                generation: generation,
                            },
                        );
                    }
                    WriteCommand::Rm(Rm { key }) => {
                        index.remove(&key);
                    }
                };
                pos = end_pos;
            }
        }

        let kv_store = SharedKvStore {
            writer: BufWriterWithPos::new(writer_file, 0),
            fragmentation: 0,
            current_generation: max_generation,
        };

        Ok(KvStore {
            index: Arc::new(index),
            store: Arc::new(Mutex::new(kv_store)),
            path: Arc::new(path),
        })
    }
}

fn log_path(dir: &Path, generation: u64) -> PathBuf {
    dir.join(format!("{}.log", generation))
}

fn get_generations(dir: &Path) -> Result<Vec<u64>> {
    let mut generations = Vec::new();

    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();

        if path.extension() != Some("log".as_ref()) {
            continue;
        }

        if let Some(generation) = path
            .file_stem()
            .and_then(|number| number.to_str())
            .and_then(|number| number.parse::<u64>().ok())
        {
            generations.push(generation);
        };
    }

    generations.sort_unstable();
    return Ok(generations);
}

fn remove_generations_below(dir: &Path, generation: u64) -> Result<()> {
    for old in get_generations(dir)? {
        if old < generation {
            std::fs::remove_file(log_path(dir, old))?;
        }
    }
    Ok(())
}
