use std::fs::File;
use crate::store::Bitcask;

mod store;
mod error;

fn main() {
    let result = Bitcask::start("/tmp/my_bitcask");

    println!("Hello, world!: {:?}", result);
}
