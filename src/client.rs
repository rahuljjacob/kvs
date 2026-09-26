use crate::shared::{CommandSerialized, Get, Response, Result, Rm, Set};
use failure::err_msg;
use std::net::TcpStream;

pub struct KvsClient {
    addr: String,
}

impl KvsClient {
    pub fn new(addr: String) -> KvsClient {
        KvsClient { addr }
    }

    pub fn rm(&self, key: String) -> Result<()> {
        match self.request(CommandSerialized::Rm(Rm { key })) {
            Ok(response) => match response {
                Response::RmResp(result) => result.map_err(err_msg),
                _ => Err(err_msg("Invalid Response")),
            },
            Err(e) => Err(e),
        }
    }

    pub fn set(&self, key: String, value: String) -> Result<()> {
        match self.request(CommandSerialized::Set(Set { key, value })) {
            Ok(response) => match response {
                Response::SetResp(result) => result.map_err(err_msg),
                _ => Err(err_msg("Invalid Response")),
            },
            Err(e) => Err(e),
        }
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        match self.request(CommandSerialized::Get(Get { key })) {
            Ok(response) => match response {
                Response::GetResp(result) => result.map_err(err_msg),
                _ => Err(err_msg("Invalid Response")),
            },
            Err(e) => Err(e),
        }
    }

    fn request(&self, command: CommandSerialized) -> Result<Response> {
        let connection = TcpStream::connect(&self.addr)?;

        serde_json::to_writer(&connection, &command)?;

        let message: Response = serde_json::Deserializer::from_reader(&connection)
            .into_iter::<Response>()
            .next()
            .unwrap()?;

        Ok(message)
    }
}
