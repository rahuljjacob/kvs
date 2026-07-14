use clap::Parser;
use kvs::Result;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Argss {
    #[arg(long, default_value = "127.0.0.1:4000")]
    addr: String,
    #[arg(long, default_value = "kvs")]
    engine: String,
}

fn main() -> Result<()> {
    let _args = Argss::parse();

    Ok(())
}
