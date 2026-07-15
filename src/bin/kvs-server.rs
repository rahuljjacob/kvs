use clap::Parser;
use kvs::Result;
use slog::{Drain, info, o};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Argss {
    #[arg(long, default_value = "127.0.0.1:4000")]
    addr: String,
    #[arg(long, default_value = "kvs")]
    engine: String,
}

fn main() -> Result<()> {
    let decorator = slog_term::TermDecorator::new().stderr().build();
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    let drain = slog_async::Async::new(drain).build().fuse();
    let logger = slog::Logger::root(drain, o!());

    let args = Argss::parse();

    info!(logger, "Starting Server"; "version" => env!("CARGO_PKG_VERSION"), "engine" => args.engine, "address" => args.addr);

    Ok(())
}
