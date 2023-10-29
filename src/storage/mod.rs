use std::collections::HashMap;
use std::mem::size_of;

use anyhow::{Context, Result};

pub use fs_store::FsStorage;

mod fs_store;

const KEY_SIZE_OFFSET: usize = 0;
const VAL_SIZE_OFFSET: usize = 4;
const KEY_OFFSET: usize = 8;

const KEY_SIZE: usize = size_of::<u32>();
const VAL_SIZE: usize = size_of::<u32>();

type KeyDir = HashMap<Vec<u8>, Header>;

#[derive(Debug)]
struct Header {
    file_id: u64,
    val_size: u32,
    val_offset: u32,
    ts_tamp: u64,
}

struct Data {
    // crc: u8
    // ts_tamp: u32,
    key_size: u32,
    val_size: u32,
    key: Vec<u8>,
    val: Vec<u8>,
}

pub trait Storage {
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()>;
    fn get(&self, key: &[u8]) -> Result<&[u8]>;
}
