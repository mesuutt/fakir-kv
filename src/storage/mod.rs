use std::collections::HashMap;
use std::mem::size_of;

use anyhow::{Context, Result};

pub use fs_store::FsStorage;

mod fs_store;


const KEY_SIZE: usize = size_of::<u32>();
const VAL_SIZE: usize = size_of::<u32>();
const CRC_SIZE: usize = size_of::<u32>();
const TS_SIZE: usize = size_of::<u32>();

const KEY_SIZE_OFFSET: usize = CRC_SIZE + TS_SIZE;
const VAL_SIZE_OFFSET: usize = CRC_SIZE + TS_SIZE + KEY_SIZE;
const KEY_OFFSET: usize = CRC_SIZE + TS_SIZE + KEY_SIZE + VAL_SIZE;

// TODO: we can use BtreeMap, it can be slower then HashMap at some cases:
// https://www.dotnetperls.com/btreemap-rust
type KeyDir = HashMap<Vec<u8>, Header>;

#[derive(Debug)]
struct Header {
    file_id: u64,
    val_size: u32,
    val_offset: u32,
    ts_tamp: u64,
}

struct Data {
    crc: u32,
    ts_tamp: u64,
    key_size: u32,
    val_size: u32,
    key: Vec<u8>,
    val: Vec<u8>,
}

pub trait Storage {
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()>;
    fn get(&mut self, key: &[u8]) -> Result<Vec<u8>> where Self: Reader;
}

pub trait Reader {
    fn read_val(&mut self, file_id: u64, offset: u32, size: u32) -> Result<Vec<u8>>;
}