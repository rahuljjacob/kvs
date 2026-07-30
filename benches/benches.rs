use criterion::Criterion;
use criterion::{criterion_group, criterion_main};
use kvs::{KvsEngine, SharedKvStore, SharedSledKvsEngine};
use rand::Rng;
use tempfile::TempDir;

fn kvs_write(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut kv_store = SharedKvStore::open(temp_dir.path()).unwrap();

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

    c.bench_function("kvs_write", |b| {
        b.iter(|| {
            for (key, value) in &key_value_map {
                assert!(kv_store.set(key.clone(), value.clone()).is_ok());
            }
        });
    });
}

fn sled_write(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut kv_store = SharedSledKvsEngine::open(temp_dir.path()).unwrap();

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

    c.bench_function("sled_write", |b| {
        b.iter(|| {
            for (key, value) in &key_values {
                assert!(kv_store.set(key.clone(), value.clone()).is_ok());
            }
        });
    });
}

fn kvs_read(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut kv_store = SharedKvStore::open(temp_dir.path()).unwrap();

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

    c.bench_function("kvs_read", |b| {
        b.iter(|| {
            for key in &key_accesses {
                assert!(kv_store.get(key.clone()).unwrap().is_some());
            }
        });
    });
}

fn sled_read(c: &mut Criterion) {
    let temp_dir = TempDir::new().unwrap();
    let mut sled_store = SharedSledKvsEngine::open(temp_dir.path()).unwrap();

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

    c.bench_function("sled_read", |b| {
        b.iter(|| {
            for key in &key_accesses {
                assert!(sled_store.get(key.clone()).unwrap().is_some());
            }
        });
    });
}

criterion_group!(benches, kvs_write, sled_write, kvs_read, sled_read);
criterion_main!(benches);
