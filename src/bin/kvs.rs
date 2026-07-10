use std::env;

use clap::{Args, Parser, Subcommand};
use kvs::Result;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Argss {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Set(SetArgs),
    Get(GetAndRemoveArgs),
    Rm(GetAndRemoveArgs),
}

#[derive(Args)]
struct SetArgs {
    key: String,
    value: String,
}

#[derive(Args)]
struct GetAndRemoveArgs {
    key: String,
}

fn main() -> Result<()> {
    let args = Argss::parse();

    let mut kv_store = kvs::KvStore::open(env::current_dir()?)?;

    match args.command {
        Command::Set(SetArgs { key, value }) => kv_store.set(key, value)?,
        Command::Get(GetAndRemoveArgs { key }) => match kv_store.get(key)? {
            Some(key) => println!("{key}"),
            None => {
                println!("Key not found");
            }
        },
        Command::Rm(GetAndRemoveArgs { key }) => {
            if kv_store.remove(key).is_err() {
                println!("Key not found");
                std::process::exit(1);
            }
        }
    };

    Ok(())
}
