extern crate core;

use crate::storage::FsStorage;
use crate::storage::Storage;

mod storage;
mod error;

fn main() {
    let mut cask = FsStorage::load("/tmp/my_bitcask").unwrap();
    cask.put(b"a1", b"a1 val");
    cask.put(b"a2", b"a2 val");

    let key = "a1";
    if let Some(x) = cask.get(key.as_bytes()).unwrap() {
        println!("key found: `{:?}`", std::str::from_utf8(x.as_slice()));
    } else {
        println!("given key not found: `{}`", key);
    }
}
