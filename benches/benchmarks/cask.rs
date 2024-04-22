use criterion::Criterion;
use rand;
use rand::seq::SliceRandom;
use tempdir::TempDir;

use fakir::storage::{Config, FsBackend, Handle};

pub fn bench(c: &mut Criterion) {
    let mut pairs: Vec<(Vec<u8>, Vec<u8>)> = (1..500).into_iter().map(|x| (format!("k_{}", x).as_bytes().to_vec(), format!("val_{}", x).as_bytes().to_vec())).collect();
    let dir = TempDir::new("bitcask-").unwrap().into_path();
    println!("storage dir: {:?}", &dir);
    let config = Config { path: dir, ..Default::default() };
    let mut cask = Handle::open(&config).unwrap();

    c.bench_function("cask.put", |b| b.iter(|| {
        for (k, v) in pairs.clone() {
            cask.put(&k, &v).unwrap();
        }
    }));

    let mut rng = rand::thread_rng();
    pairs.shuffle(&mut rng);

    c.bench_function("cask.get", |b| b.iter(|| {
        for (k, _) in pairs.clone() {
            cask.get(&k).unwrap();
        }
    }));
}