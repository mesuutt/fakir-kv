use crate::storage::FsStorage;
use crate::storage::Storage;

mod storage;
mod error;

fn main() {
    let mut cask = FsStorage::load("/tmp/my_bitcask").unwrap();
    if let Err(e) = cask.put("a1".as_bytes(), "a1 a2".as_bytes()) {
        panic!("{}", e)
    }

    println!("Hello, world!: {:?}", std::str::from_utf8(cask.get("a1".as_bytes()).unwrap().as_slice()).unwrap());
}
