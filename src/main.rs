use crate::storage::FsStorage;
use crate::storage::Storage;

mod storage;
mod error;

fn main() {
    let mut cask = FsStorage::load("/tmp/my_bitcask").unwrap();
    if let Err(e) = cask.put("asdasd".as_bytes(), "asdasdd".as_bytes()) {
        panic!("{}", e)
    }

    println!("Hello, world!: {:?}", cask);
}
