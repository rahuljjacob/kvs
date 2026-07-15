use clap::{Args, Parser, Subcommand};
use kvs::{KvsClient, Result};

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

    let kvs_client = KvsClient::new(args.addr);

    kvs_client.connect()?;

    Ok(())
}
