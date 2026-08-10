use clap::Parser;
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsServer, Result, SledKvsEngine};
use slog::{Drain, info, o};
use std::env;
use std::path::Path;

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

    check_engine(&env::current_dir()?, &args.engine)?;

    info!(&logger, "Starting Server"; "version" => env!("CARGO_PKG_VERSION"), "engine" => &args.engine, "address" => &args.addr);

    if args.engine == "kvs" {
        let engine = KvStore::open(env::current_dir()?)?;
        let thread_pool = SharedQueueThreadPool::new(num_cpus::get() as u32)?;
        let kv_server = KvsServer::new(args.addr, engine, logger, thread_pool);
        kv_server.request_loop()?
    } else if args.engine == "sled" {
        let engine = SledKvsEngine::open(env::current_dir()?)?;
        let thread_pool = SharedQueueThreadPool::new(num_cpus::get() as u32)?;
        let kv_server = KvsServer::new(args.addr, engine, logger, thread_pool);
        kv_server.request_loop()?
    } else {
        return Err(failure::format_err!("What engine bruh"));
    }

    Ok(())
}

fn check_engine(path: &Path, engine: &str) -> Result<()> {
    let marker_path = path.join("engine");
    let existing = std::fs::read_to_string(&marker_path).unwrap_or_default();
    let existing = existing.trim();

    if existing.is_empty() {
        std::fs::write(marker_path, engine)?;
    } else if existing != engine {
        return Err(failure::format_err!("Wrong engine for this directory"));
    }

    Ok(())
}
