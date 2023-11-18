use criterion::Criterion;
use tempdir::TempDir;

use bitcask::FsStorage;
use bitcask::Storage;

pub fn bench(c: &mut Criterion) {
    let pairs: Vec<(Vec<u8>, Vec<u8>)> = (1..500).into_iter().map(|x| (format!("k_{}", x).as_bytes().to_vec(), format!("val_{}", x).as_bytes().to_vec())).collect();
    let dir = TempDir::new("bitcask-").unwrap();
    let mut cask = FsStorage::load(dir.path().to_str().unwrap()).unwrap();

    c.bench_function("cask.put", |b| b.iter(|| {
        for (k, v) in pairs.clone() {
            cask.put(&k, &v).unwrap();
        }
    }));

    c.bench_function("cask.get", |b| b.iter(|| {
        for (k, v) in pairs.clone() {
            cask.get(&k).unwrap();
        }
    }));

}