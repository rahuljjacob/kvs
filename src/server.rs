use crate::shared::Response;
pub use crate::shared::{CommandSerialized, Get, Result, Rm, Set};
use crate::thread_pool::ThreadPool;
use slog::{Logger, error};
use std::net::{TcpListener, TcpStream};

pub struct KvsServer<E: KvsEngine, P: ThreadPool> {
    pub addr: String,
    pub engine: E,
    pub logger: Logger,
    pub thread_pool: P,
}

pub trait KvsEngine: Clone + Send + 'static {
    fn get(&self, key: String) -> Result<Option<String>>;

    fn set(&self, key: String, value: String) -> Result<()>;

    fn remove(&self, key: String) -> Result<()>;
}

impl<E: KvsEngine, P: ThreadPool> KvsServer<E, P> {
    pub fn new(addr: String, engine: E, logger: Logger, thread_pool: P) -> Self {
        KvsServer {
            addr,
            engine,
            logger,
            thread_pool,
        }
    }

    pub fn request_loop(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr)?;

        for stream in listener.incoming() {
            match stream {
                Ok(connection) => {
                    let engine = self.engine.clone();
                    let logger = self.logger.clone();
                    self.thread_pool.spawn(move || {
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
    let command: CommandSerialized = serde_json::Deserializer::from_reader(&connection)
        .into_iter::<CommandSerialized>()
        .next()
        .unwrap()?;

    match command {
        CommandSerialized::Get(Get { key }) => {
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
        CommandSerialized::Set(Set { key, value }) => {
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
        CommandSerialized::Rm(Rm { key }) => {
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
