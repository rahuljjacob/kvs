use failure::Error;
use serde::{Deserialize, Serialize};
use slog::{Logger, info};
use std::net::{TcpListener, TcpStream};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Seek, SeekFrom, Write},
    path::PathBuf,
};

pub struct KvStore {
    store: HashMap<String, (u64, u64)>,
    path: PathBuf,
    uncompacted: u64,
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

pub struct KvsServer {
    addr: String,
    engine: String,
    logger: Logger,
}

pub struct KvsClient {
    addr: String,
}

#[derive(Serialize, Deserialize)]
enum WriteCommand {
    Set(Set),
    Rm(Rm),
}

const THRESHOLD: u64 = 1 * 1024 * 1024;

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

        Ok(kv_store)
    }

    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        let mut file = File::open(&self.path)?;
        let Some(offset) = self.store.get(&key).copied() else {
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
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let mut file = File::options().append(true).create(true).open(&self.path)?;
        let offset = file.seek(SeekFrom::End(0))?;

        let set_op = WriteCommand::Set(Set {
            key: key.clone(),
            value,
        });
        let serialized = serde_json::to_string(&set_op)?;
        file.write_all(serialized.as_bytes())?;
        let end_offset = file.stream_position()?;
        let len = end_offset - offset;

        if let Some(old_value) = self.store.get(&key) {
            self.uncompacted = self.uncompacted + old_value.1;
        }
        self.store.insert(key, (offset, len));

        if self.uncompacted > THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    // Serialize key and write
    pub fn remove(&mut self, key: String) -> Result<()> {
        if let Some(old_value) = self.store.get(&key) {
            self.uncompacted = self.uncompacted + old_value.1
        } else {
            return Err(failure::format_err!("Key not found"));
        }

        self.store.remove(&key);
        let del_op = WriteCommand::Rm(Rm { key });
        let serialized = serde_json::to_string(&del_op)?;
        let mut file = File::options().append(true).create(true).open(&self.path)?;
        file.write_all(serialized.as_bytes())?;

        if self.uncompacted > THRESHOLD {
            self.compact()?;
        }

        Ok(())
    }

    pub fn compact(&mut self) -> Result<()> {
        let path = self.path.clone();
        let path = path.with_file_name("db2.txt");
        let mut new_store: HashMap<String, (u64, u64)> = HashMap::new();

        let mut file = File::options()
            .append(true)
            .read(true)
            .create(true)
            .open(&path)?;

        for (key, _) in self.store.clone() {
            let value = self.get(key.clone())?.unwrap();
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

impl KvsServer {
    pub fn new(addr: String, engine: String, logger: Logger) -> KvsServer {
        KvsServer {
            addr,
            engine,
            logger,
        }
    }

    pub fn accept_clients(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr)?;

        for _stream in listener.incoming() {
            info!(&self.logger, "Client connected");
        }

        Ok(())
    }
}

impl KvsClient {
    pub fn new(addr: String) -> KvsClient {
        KvsClient { addr }
    }

    pub fn connect(&self) -> Result<()> {
        let _connection = TcpStream::connect(&self.addr)?;

        println!("Success");
        Ok(())
    }
}
