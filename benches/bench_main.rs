
use criterion::{criterion_group, criterion_main};

mod benchmarks;

criterion_group!(all_benches,
    benchmarks::cask::bench,
);

criterion_main!(all_benches);