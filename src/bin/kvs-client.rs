use clap::{Args, Parser, Subcommand};
use kvs::{CommandSerialized, Get, KvsClient, Result, Rm, Set};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Argss {
    #[command(subcommand)]
    command: Command,
    #[arg(long, default_value = "127.0.0.1:4000", global = true)]
    addr: String,
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

    let command_to_send = match args.command {
        Command::Set(SetArgs { key, value }) => CommandSerialized::Set(Set { key, value }),
        Command::Get(GetAndRemoveArgs { key }) => CommandSerialized::Get(Get { key }),
        Command::Rm(GetAndRemoveArgs { key }) => CommandSerialized::Rm(Rm { key }),
    };

    let kvs_client = KvsClient::new(args.addr);

    kvs_client.send(command_to_send)?;

    Ok(())
}
