use clap::{Args, Parser, Subcommand};
use kvs::Result;
use serde::{Deserialize, Serialize};

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
    let _args = Argss::parse();

    eprintln!("unimplemented");
    std::process::exit(1);
}
