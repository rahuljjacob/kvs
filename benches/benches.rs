use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use crossbeam_utils::sync::WaitGroup;
use kvs::thread_pool::{SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsClient, KvsEngine, KvsServer, SledKvsEngine};
use rand::Rng;
use slog::{Discard, Logger, o};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

fn kvs_write(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut kv_store = KvStore::open(temp_dir.path()).unwrap();

    let mut rng = rand::thread_rng();

    let key_value_map: Vec<(String, String)> = (0..100)
        .map(|_| {
            let key_length = rng.gen_range(1, 100000);
            let val_length = rng.gen_range(1, 100000);
            let key: String = (0..key_length).map(|_| rng.r#gen::<char>()).collect();
            let val: String = (0..val_length).map(|_| rng.r#gen::<char>()).collect();
            (key, val)
        })
        .collect();

    c.bench_function("kvs_write", move |b| {
        b.iter(|| {
            for (key, value) in &key_value_map {
                assert!(kv_store.set(key.clone(), value.clone()).is_ok());
            }
        });
    });
}

fn sled_write(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut kv_store = SledKvsEngine::open(temp_dir.path()).unwrap();

    let mut rng = rand::thread_rng();

    let key_values: Vec<(String, String)> = (0..100)
        .map(|_| {
            let key_length = rng.gen_range(1, 100000);
            let val_length = rng.gen_range(1, 100000);
            let key: String = (0..key_length).map(|_| rng.r#gen::<char>()).collect();
            let val: String = (0..val_length).map(|_| rng.r#gen::<char>()).collect();
            (key, val)
        })
        .collect();

    c.bench_function("sled_write", move |b| {
        b.iter(|| {
            for (key, value) in &key_values {
                assert!(kv_store.set(key.clone(), value.clone()).is_ok());
            }
        });
    });
}

fn kvs_read(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut kv_store = KvStore::open(temp_dir.path()).unwrap();

    let mut rng = rand::thread_rng();

    let keys: Vec<String> = (0..1000)
        .map(|_| {
            let key_length = rng.gen_range(1, 100000);
            let val_length = rng.gen_range(1, 100000);
            let key: String = (0..key_length).map(|_| rng.r#gen::<char>()).collect();
            let val: String = (0..val_length).map(|_| rng.r#gen::<char>()).collect();

            kv_store.set(key.clone(), val).unwrap();

            key
        })
        .collect();

    let key_accesses: Vec<String> = (0..1000)
        .map(|_| keys[rng.gen_range(0, 1000)].clone())
        .collect();

    c.bench_function("kvs_read", move |b| {
        b.iter(|| {
            for key in &key_accesses {
                assert!(kv_store.get(key.clone()).unwrap().is_some());
            }
        });
    });
}

fn sled_read(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut sled_store = SledKvsEngine::open(temp_dir.path()).unwrap();

    let mut rng = rand::thread_rng();

    let keys: Vec<String> = (0..1000)
        .map(|_| {
            let key_length = rng.gen_range(1, 100000);
            let val_length = rng.gen_range(1, 100000);
            let key: String = (0..key_length).map(|_| rng.r#gen::<char>()).collect();
            let val: String = (0..val_length).map(|_| rng.r#gen::<char>()).collect();

            sled_store.set(key.clone(), val).unwrap();

            key
        })
        .collect();

    let key_accesses: Vec<String> = (0..1000)
        .map(|_| keys[rng.gen_range(0, 1000)].clone())
        .collect();

    c.bench_function("sled_read", move |b| {
        b.iter(|| {
            for key in &key_accesses {
                assert!(sled_store.get(key.clone()).unwrap().is_some());
            }
        });
    });
}

fn write_queued_kvstore(c: &mut Criterion) {
    let max_threads = 2 * num_cpus::get();
    let inputs: Vec<usize> = (1..=max_threads).step_by(2).collect();

    c.bench_function_over_inputs(
        "write_queued_kvstore",
        |b, &num_threads| {
            // --- setup: not measured ---
            let temp_dir = TempDir::new().unwrap();
            let engine = KvStore::open(temp_dir.path()).unwrap();
            static PORT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(5000);
            let port = PORT.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let addr = format!("127.0.0.1:{}", port);
            let server_pool = SharedQueueThreadPool::new(num_threads as u32).unwrap();
            let logger = Logger::root(Discard, o!());
            let server = KvsServer::new(addr.clone(), engine, logger, server_pool);

            thread::spawn(move || server.request_loop().unwrap());
            thread::sleep(Duration::from_millis(100)); // let it bind

            let keys: Vec<String> = (0..1000).map(|i| format!("key{:08}", i)).collect();
            let client_pool = SharedQueueThreadPool::new(1000).unwrap();

            // --- measured ---
            b.iter(|| {
                let wg = WaitGroup::new();
                for key in &keys {
                    let wg = wg.clone();
                    let key = key.clone();
                    let addr = addr.clone();
                    client_pool.spawn(move || {
                        let client = KvsClient::new(addr);
                        assert!(client.set(key, "value".to_string()).is_ok());
                        drop(wg);
                    });
                }
                wg.wait();
            });
        },
        inputs,
    );
}

criterion_group!(
    benches,
    kvs_write,
    sled_write,
    kvs_read,
    sled_read,
    write_queued_kvstore
);
criterion_main!(benches);
