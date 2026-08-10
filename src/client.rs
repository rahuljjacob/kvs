use crate::shared::{CommandSerialized, Response, Result};
use failure::err_msg;
use std::net::TcpStream;

pub struct KvsClient {
    addr: String,
}

impl KvsClient {
    pub fn new(addr: String) -> KvsClient {
        KvsClient { addr }
    }

    pub fn send(&self, command: CommandSerialized) -> Result<()> {
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
