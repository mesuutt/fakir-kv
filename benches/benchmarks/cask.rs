use criterion::Criterion;
use tempdir::TempDir;

use bitcask::FsStorage;
use bitcask::Storage;

pub fn bench(c: &mut Criterion) {
    let pairs: Vec<(String, String)> = (1..100).into_iter().map(|x| (format!("k_{}", x), format!("val_{}", x))).collect();
    let dir = TempDir::new("bitcask-").unwrap();
    let mut cask = FsStorage::load(dir.path().to_str().unwrap()).unwrap();

    c.bench_function("cask.put", |b| b.iter(|| {
        for (k, v) in pairs.clone() {
            cask.put(k.as_bytes(), v.as_bytes()).unwrap();
            cask.get(k.as_bytes()).unwrap();
        }
    }));
}