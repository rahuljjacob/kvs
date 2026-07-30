use failure::{Error, err_msg};
use serde::{Deserialize, Serialize};
use sled::Db;
use slog::{Logger, error};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufReader, Seek, SeekFrom, Write},
    path::PathBuf,
};
use thread_pool::NaiveThreadPool;

use crate::thread_pool::ThreadPool;

const THRESHOLD: u64 = 1 * 1024 * 1024;

pub type Result<T> = std::result::Result<T, Error>;

pub mod thread_pool;

pub struct SharedKvStore {
    store: HashMap<String, (u64, u64)>,
    path: PathBuf,
    uncompacted: u64,
}

#[derive(Clone)]
pub struct KvStore(Arc<Mutex<SharedKvStore>>);

pub struct SharedSledKvsEngine {
    pub store: Db,
}

#[derive(Clone)]
pub struct SledKvsEngine(Arc<Mutex<SharedSledKvsEngine>>);

#[derive(Serialize, Deserialize)]
pub struct Set {
    pub key: String,
    pub value: String,
}

#[derive(Serialize, Deserialize)]
pub struct Rm {
    pub key: String,
}

#[derive(Serialize, Deserialize)]
pub struct Get {
    pub key: String,
}

pub struct KvsServer<E: KvsEngine> {
    pub addr: String,
    pub engine: E,
    pub logger: Logger,
}

pub struct KvsClient {
    addr: String,
}

pub trait KvsEngine: Clone + Send + 'static {
    fn get(&self, key: String) -> Result<Option<String>>;

    fn set(&self, key: String, value: String) -> Result<()>;

    fn remove(&self, key: String) -> Result<()>;
}

#[derive(Serialize, Deserialize)]
enum Response {
    SetResp(std::result::Result<(), String>),
    RmResp(std::result::Result<(), String>),
    GetResp(std::result::Result<Option<String>, String>),
}

#[derive(Serialize, Deserialize)]
enum WriteCommand {
    Set(Set),
    Rm(Rm),
}

#[derive(Serialize, Deserialize)]
pub enum CommandSer {
    Set(Set),
    Get(Get),
    Rm(Rm),
}

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

impl KvsEngine for SledKvsEngine {
    fn get(&self, key: String) -> Result<Option<String>> {
        let sled_store = self.0.lock().unwrap();
        let result = sled_store.store.get(key)?;
        match result {
            Some(value) => Ok(Some(String::from_utf8(value.to_vec())?)),
            None => Ok(None),
        }
    }

    fn set(&self, key: String, value: String) -> Result<()> {
        let sled_store = self.0.lock().unwrap();
        sled_store.store.insert(key, value.as_bytes())?;
        sled_store.store.flush()?;
        Ok(())
    }

    fn remove(&self, key: String) -> Result<()> {
        let sled_store = self.0.lock().unwrap();
        let result = sled_store.store.remove(key)?;
        sled_store.store.flush()?;
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
        Ok(SledKvsEngine(Arc::new(Mutex::new(SharedSledKvsEngine {
            store: db,
        }))))
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

impl<E: KvsEngine> KvsServer<E> {
    pub fn request_loop(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr)?;
        let thread_pool = NaiveThreadPool::new(8)?;

        for stream in listener.incoming() {
            match stream {
                Ok(connection) => {
                    let engine = self.engine.clone();
                    let logger = self.logger.clone();
                    thread_pool.spawn(move || {
                        if let Err(e) = handle_request(engine, connection) {
                            error!(logger, "Request Failed to execute"; "err" => e.to_string())
                        }
                    });
                }
                Err(e) => error!(self.logger, "Connection Failed"; "err" => e.to_string()),
            }
        }

        Ok(())
    }
}

fn handle_request<E: KvsEngine>(engine: E, connection: TcpStream) -> Result<()> {
    let command: CommandSer = serde_json::Deserializer::from_reader(&connection)
        .into_iter::<CommandSer>()
        .next()
        .unwrap()?;

    match command {
        CommandSer::Get(Get { key }) => {
            let res = engine.get(key);
            match res {
                Ok(val) => {
                    match val {
                        Some(value) => {
                            serde_json::to_writer(
                                &connection,
                                &Response::GetResp(Ok(Some(value))),
                            )?;
                        }
                        None => {
                            // Send Key not found : String
                            serde_json::to_writer(&connection, &Response::GetResp(Ok(None)))?;
                        }
                    }
                }
                Err(e) => {
                    serde_json::to_writer(&connection, &Response::GetResp(Err(e.to_string())))?;
                }
            }
        }
        CommandSer::Set(Set { key, value }) => {
            let res = engine.set(key, value);
            match res {
                Ok(()) => {
                    let response = Response::SetResp(Ok(()));
                    serde_json::to_writer(&connection, &response)?;
                }
                Err(e) => {
                    serde_json::to_writer(&connection, &Response::SetResp(Err(e.to_string())))?;
                }
            }
        }
        CommandSer::Rm(Rm { key }) => {
            let res = engine.remove(key);
            match res {
                Ok(()) => {
                    let response = Response::RmResp(Ok(()));
                    serde_json::to_writer(&connection, &response)?;
                }
                Err(e) => {
                    serde_json::to_writer(&connection, &Response::RmResp(Err(e.to_string())))?;
                }
            }
        }
    };

    Ok(())
}

impl KvsClient {
    pub fn new(addr: String) -> KvsClient {
        KvsClient { addr }
    }

    pub fn send(&self, command: CommandSer) -> Result<()> {
        let connection = TcpStream::connect(&self.addr)?;

        serde_json::to_writer(&connection, &command)?;

        let message: Response = serde_json::Deserializer::from_reader(&connection)
            .into_iter::<Response>()
            .next()
            .unwrap()?;

        match message {
            Response::GetResp(result) => match result {
                Ok(value) => match value {
                    Some(val) => {
                        println!("{}", &val);
                        Ok(())
                    }

                    None => {
                        println!("Key not found");
                        Ok(())
                    }
                },
                Err(e) => {
                    return Err(err_msg(e));
                }
            },
            Response::RmResp(result) => match result {
                Ok(_) => Ok(()),
                Err(e) => {
                    return Err(err_msg(e));
                }
            },
            Response::SetResp(result) => match result {
                Ok(_) => Ok(()),

                Err(e) => {
                    return Err(err_msg(e));
                }
            },
        }
    }
}
