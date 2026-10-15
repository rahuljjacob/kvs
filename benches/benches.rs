use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use crossbeam_utils::sync::WaitGroup;
use kvs::thread_pool::{RayonThreadPool, SharedQueueThreadPool, ThreadPool};
use kvs::{KvStore, KvsClient, KvsEngine, KvsServer, SledKvsEngine};
use rand::Rng;
use slog::{Discard, Logger, o};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;
use tempfile::TempDir;

// Common PORT
static PORT: AtomicUsize = AtomicUsize::new(5000);

fn thread_counts() -> Vec<usize> {
    let max_threads = 2 * num_cpus::get();
    let mut inputs = vec![1];
    inputs.extend((2..=max_threads).step_by(2));
    inputs
}

fn kvs_write(c: &mut Criterion) {
    let mut state = None;

    c.bench_function("kvs_write", move |b| {
        let (_temp_dir, kv_store, key_value_map) = state.get_or_insert_with(|| {
            let temp_dir = TempDir::new().unwrap();
            let kv_store = KvStore::open(temp_dir.path()).unwrap();

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

            (temp_dir, kv_store, key_value_map)
        });

        b.iter(|| {
            for (key, value) in key_value_map.iter() {
                assert!(kv_store.set(key.clone(), value.clone()).is_ok());
            }
        });
    });
}

fn sled_write(c: &mut Criterion) {
    let mut state = None;

    c.bench_function("sled_write", move |b| {
        let (_temp_dir, kv_store, key_values) = state.get_or_insert_with(|| {
            let temp_dir = TempDir::new().unwrap();
            let kv_store = SledKvsEngine::open(temp_dir.path()).unwrap();

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

            (temp_dir, kv_store, key_values)
        });

        b.iter(|| {
            for (key, value) in key_values.iter() {
                assert!(kv_store.set(key.clone(), value.clone()).is_ok());
            }
        });
    });
}

fn kvs_read(c: &mut Criterion) {
    let mut state = None;

    c.bench_function("kvs_read", move |b| {
        let (_temp_dir, kv_store, key_accesses) = state.get_or_insert_with(|| {
            let temp_dir = TempDir::new().unwrap();
            let kv_store = KvStore::open(temp_dir.path()).unwrap();

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

            (temp_dir, kv_store, key_accesses)
        });

        b.iter(|| {
            for key in key_accesses.iter() {
                assert!(kv_store.get(key.clone()).unwrap().is_some());
            }
        });
    });
}

fn sled_read(c: &mut Criterion) {
    let mut state = None;

    c.bench_function("sled_read", move |b| {
        let (_temp_dir, sled_store, key_accesses) = state.get_or_insert_with(|| {
            let temp_dir = TempDir::new().unwrap();
            let sled_store = SledKvsEngine::open(temp_dir.path()).unwrap();

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

            (temp_dir, sled_store, key_accesses)
        });

        b.iter(|| {
            for key in key_accesses.iter() {
                assert!(sled_store.get(key.clone()).unwrap().is_some());
            }
        });
    });
}

fn write_queued_kvstore(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "write_queued_kvstore",
        |b, &num_threads| {
            // --- setup: not measured ---
            let temp_dir = TempDir::new().unwrap();
            let engine = KvStore::open(temp_dir.path()).unwrap();
            let port = PORT.fetch_add(1, Ordering::Relaxed);
            let addr = format!("127.0.0.1:{}", port);
            let server_pool = SharedQueueThreadPool::new(num_threads as u32).unwrap();
            let logger = Logger::root(Discard, o!());
            let server = KvsServer::new(addr.clone(), engine, logger, server_pool);

            let shutdown_handle = server.shutdown_handle();

            let server_thread = thread::spawn(move || server.request_loop().unwrap());
            thread::sleep(Duration::from_millis(100));

            let keys: Vec<String> = (0..1000).map(|i| format!("key{:08}", i)).collect();
            let client_pool = SharedQueueThreadPool::new(1000).unwrap();
            let errors = Arc::new(AtomicUsize::new(0));
            // --- measured ---
            b.iter(|| {
                let wg = WaitGroup::new();
                for key in &keys {
                    let wg = wg.clone();
                    let errors = Arc::clone(&errors);
                    let key = key.clone();
                    let addr = addr.clone();
                    client_pool.spawn(move || {
                        let client = KvsClient::new(addr);
                        if let Err(e) = client.set(key, "value".to_string()) {
                            if errors.fetch_add(1, Ordering::Relaxed) == 0 {
                                eprintln!("set failed: {}", e);
                            }
                        }
                        drop(wg);
                    });
                }
                wg.wait();
                let failed = errors.load(Ordering::Relaxed);
                assert_eq!(failed, 0, "{} of {} requests failed", failed, keys.len());
            });

            shutdown_handle.shutdown_server().unwrap();
            server_thread.join().unwrap();
        },
        thread_counts(),
    );
}

fn read_queued_kvstore(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "read_queued_kvstore",
        |b, &num_threads| {
            let temp_dir = TempDir::new().unwrap();
            let engine = KvStore::open(temp_dir.path()).unwrap();
            let port = PORT.fetch_add(1, Ordering::Relaxed);
            let addr = format!("127.0.0.1:{}", port);
            let server_pool = SharedQueueThreadPool::new(num_threads as u32).unwrap();
            let logger = Logger::root(Discard, o!());
            let server = KvsServer::new(addr.clone(), engine, logger, server_pool);

            let shutdown_handle = server.shutdown_handle();

            let server_thread = thread::spawn(move || server.request_loop().unwrap());
            thread::sleep(Duration::from_millis(100));

            let keys: Vec<String> = (0..1000).map(|i| format!("key{:08}", i)).collect();
            let client_pool = SharedQueueThreadPool::new(1000).unwrap();
            let errors = Arc::new(AtomicUsize::new(0));

            let setup_client = KvsClient::new(addr.clone());
            for key in &keys {
                setup_client.set(key.clone(), "value".to_string()).unwrap();
            }

            b.iter(|| {
                let wg = WaitGroup::new();
                for key in &keys {
                    let wg = wg.clone();
                    let errors = Arc::clone(&errors);
                    let key = key.clone();
                    let addr = addr.clone();
                    client_pool.spawn(move || {
                        let client = KvsClient::new(addr);
                        match client.get(key) {
                            Ok(Some(ref value)) if value == "value" => {}
                            other => {
                                if errors.fetch_add(1, Ordering::Relaxed) == 0 {
                                    eprintln!("get failed: {:?}", other);
                                }
                            }
                        }
                        drop(wg);
                    });
                }
                wg.wait();
                let failed = errors.load(Ordering::Relaxed);
                assert_eq!(failed, 0, "{} of {} requests failed", failed, keys.len());
            });

            shutdown_handle.shutdown_server().unwrap();
            server_thread.join().unwrap();
        },
        thread_counts(),
    );
}

fn write_rayon_kvstore(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "write_rayon_kvstore",
        |b, &num_threads| {
            let temp_dir = TempDir::new().unwrap();
            let engine = KvStore::open(temp_dir.path()).unwrap();
            let port = PORT.fetch_add(1, Ordering::Relaxed);
            let addr = format!("127.0.0.1:{}", port);
            let server_pool = RayonThreadPool::new(num_threads as u32).unwrap();
            let logger = Logger::root(Discard, o!());
            let server = KvsServer::new(addr.clone(), engine, logger, server_pool);

            let shutdown_handle = server.shutdown_handle();

            let server_thread = thread::spawn(move || server.request_loop().unwrap());
            thread::sleep(Duration::from_millis(100));

            let keys: Vec<String> = (0..1000).map(|i| format!("key{:08}", i)).collect();
            let client_pool = SharedQueueThreadPool::new(1000).unwrap();
            let errors = Arc::new(AtomicUsize::new(0));

            b.iter(|| {
                let wg = WaitGroup::new();
                for key in &keys {
                    let wg = wg.clone();
                    let errors = Arc::clone(&errors);
                    let key = key.clone();
                    let addr = addr.clone();
                    client_pool.spawn(move || {
                        let client = KvsClient::new(addr);
                        if let Err(e) = client.set(key, "value".to_string()) {
                            if errors.fetch_add(1, Ordering::Relaxed) == 0 {
                                eprintln!("set failed: {}", e);
                            }
                        }
                        drop(wg);
                    });
                }
                wg.wait();
                let failed = errors.load(Ordering::Relaxed);
                assert_eq!(failed, 0, "{} of {} requests failed", failed, keys.len());
            });

            shutdown_handle.shutdown_server().unwrap();
            server_thread.join().unwrap();
        },
        thread_counts(),
    );
}

fn read_rayon_kvstore(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "read_rayon_kvstore",
        |b, &num_threads| {
            let temp_dir = TempDir::new().unwrap();
            let engine = KvStore::open(temp_dir.path()).unwrap();
            let port = PORT.fetch_add(1, Ordering::Relaxed);
            let addr = format!("127.0.0.1:{}", port);
            let server_pool = RayonThreadPool::new(num_threads as u32).unwrap();
            let logger = Logger::root(Discard, o!());
            let server = KvsServer::new(addr.clone(), engine, logger, server_pool);

            let shutdown_handle = server.shutdown_handle();

            let server_thread = thread::spawn(move || server.request_loop().unwrap());
            thread::sleep(Duration::from_millis(100));

            let keys: Vec<String> = (0..1000).map(|i| format!("key{:08}", i)).collect();
            let client_pool = SharedQueueThreadPool::new(1000).unwrap();
            let errors = Arc::new(AtomicUsize::new(0));

            let setup_client = KvsClient::new(addr.clone());
            for key in &keys {
                setup_client.set(key.clone(), "value".to_string()).unwrap();
            }

            b.iter(|| {
                let wg = WaitGroup::new();
                for key in &keys {
                    let wg = wg.clone();
                    let errors = Arc::clone(&errors);
                    let key = key.clone();
                    let addr = addr.clone();
                    client_pool.spawn(move || {
                        let client = KvsClient::new(addr);
                        match client.get(key) {
                            Ok(Some(ref value)) if value == "value" => {}
                            other => {
                                if errors.fetch_add(1, Ordering::Relaxed) == 0 {
                                    eprintln!("get failed: {:?}", other);
                                }
                            }
                        }
                        drop(wg);
                    });
                }
                wg.wait();
                let failed = errors.load(Ordering::Relaxed);
                assert_eq!(failed, 0, "{} of {} requests failed", failed, keys.len());
            });

            shutdown_handle.shutdown_server().unwrap();
            server_thread.join().unwrap();
        },
        thread_counts(),
    );
}

fn write_rayon_sledkvengine(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "write_rayon_sledkvengine",
        |b, &num_threads| {
            let temp_dir = TempDir::new().unwrap();
            let engine = SledKvsEngine::open(temp_dir.path()).unwrap();
            let port = PORT.fetch_add(1, Ordering::Relaxed);
            let addr = format!("127.0.0.1:{}", port);
            let server_pool = RayonThreadPool::new(num_threads as u32).unwrap();
            let logger = Logger::root(Discard, o!());
            let server = KvsServer::new(addr.clone(), engine, logger, server_pool);

            let shutdown_handle = server.shutdown_handle();

            let server_thread = thread::spawn(move || server.request_loop().unwrap());
            thread::sleep(Duration::from_millis(100));

            let keys: Vec<String> = (0..1000).map(|i| format!("key{:08}", i)).collect();
            let client_pool = SharedQueueThreadPool::new(1000).unwrap();
            let errors = Arc::new(AtomicUsize::new(0));

            b.iter(|| {
                let wg = WaitGroup::new();
                for key in &keys {
                    let wg = wg.clone();
                    let errors = Arc::clone(&errors);
                    let key = key.clone();
                    let addr = addr.clone();
                    client_pool.spawn(move || {
                        let client = KvsClient::new(addr);
                        if let Err(e) = client.set(key, "value".to_string()) {
                            if errors.fetch_add(1, Ordering::Relaxed) == 0 {
                                eprintln!("set failed: {}", e);
                            }
                        }
                        drop(wg);
                    });
                }
                wg.wait();
                let failed = errors.load(Ordering::Relaxed);
                assert_eq!(failed, 0, "{} of {} requests failed", failed, keys.len());
            });

            shutdown_handle.shutdown_server().unwrap();
            server_thread.join().unwrap();
        },
        thread_counts(),
    );
}

fn read_rayon_sledkvengine(c: &mut Criterion) {
    c.bench_function_over_inputs(
        "read_rayon_sledkvengine",
        |b, &num_threads| {
            let temp_dir = TempDir::new().unwrap();
            let engine = SledKvsEngine::open(temp_dir.path()).unwrap();
            let port = PORT.fetch_add(1, Ordering::Relaxed);
            let addr = format!("127.0.0.1:{}", port);
            let server_pool = RayonThreadPool::new(num_threads as u32).unwrap();
            let logger = Logger::root(Discard, o!());
            let server = KvsServer::new(addr.clone(), engine, logger, server_pool);

            let shutdown_handle = server.shutdown_handle();

            let server_thread = thread::spawn(move || server.request_loop().unwrap());
            thread::sleep(Duration::from_millis(100));

            let keys: Vec<String> = (0..1000).map(|i| format!("key{:08}", i)).collect();
            let client_pool = SharedQueueThreadPool::new(1000).unwrap();
            let errors = Arc::new(AtomicUsize::new(0));

            let setup_client = KvsClient::new(addr.clone());
            for key in &keys {
                setup_client.set(key.clone(), "value".to_string()).unwrap();
            }

            b.iter(|| {
                let wg = WaitGroup::new();
                for key in &keys {
                    let wg = wg.clone();
                    let errors = Arc::clone(&errors);
                    let key = key.clone();
                    let addr = addr.clone();
                    client_pool.spawn(move || {
                        let client = KvsClient::new(addr);
                        match client.get(key) {
                            Ok(Some(ref value)) if value == "value" => {}
                            other => {
                                if errors.fetch_add(1, Ordering::Relaxed) == 0 {
                                    eprintln!("get failed: {:?}", other);
                                }
                            }
                        }
                        drop(wg);
                    });
                }
                wg.wait();
                let failed = errors.load(Ordering::Relaxed);
                assert_eq!(failed, 0, "{} of {} requests failed", failed, keys.len());
            });

            shutdown_handle.shutdown_server().unwrap();
            server_thread.join().unwrap();
        },
        thread_counts(),
    );
}

criterion_group!(benches, kvs_write, sled_write, kvs_read, sled_read);

criterion_group! {
    name = server_benches;
    config = Criterion::default().sample_size(5);
    targets =
        write_queued_kvstore,
        read_queued_kvstore,
        write_rayon_kvstore,
        read_rayon_kvstore,
        write_rayon_sledkvengine,
        read_rayon_sledkvengine
}

criterion_main!(benches, server_benches);
