use serde::{Deserialize, Serialize};
use failure::Error;

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

#[derive(Serialize, Deserialize)]
pub enum Response {
    SetResp(std::result::Result<(), String>),
    RmResp(std::result::Result<(), String>),
    GetResp(std::result::Result<Option<String>, String>),
}

#[derive(Serialize, Deserialize)]
pub enum WriteCommand {
    Set(Set),
    Rm(Rm),
}

#[derive(Serialize, Deserialize)]
pub enum CommandSerialized {
    Set(Set),
    Get(Get),
    Rm(Rm),
}

pub type Result<T> = std::result::Result<T, Error>;
