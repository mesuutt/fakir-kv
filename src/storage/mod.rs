use std::collections::HashMap;
use std::mem::size_of;

use anyhow::Context;

pub use fs_store::FsStorage;

mod fs_store;
mod file_lock;

// [crc|ts_tamp|ksz|vsz|key|val]

const CRC_SIZE: usize = size_of::<u32>();
const TS_SIZE: usize = size_of::<u32>();
const KEY_SIZE: usize = size_of::<u32>();
const VAL_SIZE: usize = size_of::<u32>();

const CRC_OFFSET: usize = 0;
const TS_OFFSET: usize = CRC_SIZE;
const KEY_SIZE_OFFSET: usize = CRC_SIZE + TS_SIZE;
const VAL_SIZE_OFFSET: usize = KEY_SIZE_OFFSET + KEY_SIZE;
const KEY_OFFSET: usize = VAL_SIZE_OFFSET + VAL_SIZE;

// TODO: we can use BtreeMap, it can be slower then HashMap at some cases:
// https://www.dotnetperls.com/btreemap-rust
type KeyDir = HashMap<Vec<u8>, Header>;

#[derive(Debug)]
struct Header {
    file_id: u64,
    val_size: u32,
    val_offset: u32,
    ts_tamp: u32,
}

struct Data {
    crc: u32,
    ts_tamp: u64,
    key_size: u32,
    val_size: u32,
    key: Vec<u8>,
    val: Vec<u8>,
}

impl Data {
    pub fn from(p: &[u8]) -> Self {
        Data {
            crc: 0,
            ts_tamp: 0,
            key_size: 0,
            val_size: 0,
            key: vec![],
            val: vec![],
        }
    }
}

/*pub trait Reader {
    fn read_val(&mut self, file_id: u64, offset: u32, size: u32) -> anyhow::Result<Vec<u8>>;
}

pub trait Writer {
    fn write(&mut self, buf: &[u8]) -> anyhow::Result<()>;
}
*/