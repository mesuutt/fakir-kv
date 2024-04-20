use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::mem::size_of;

pub use fs_store::FsStorage;
pub use handle::Handle;
pub use config::Config;
pub use context::WriteContext;

mod fs_store;
mod file_lock;
mod utils;
mod log_reader;
mod handle;
mod config;
mod context;
mod log_writer;

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

// use backspace char as tombstone marker
const TOMBSTONE_MARKER_CHAR: u8 = 8;

// TODO: We can benchmark BtreeMap: https://www.dotnetperls.com/btreemap-rust
type KeyDir = HashMap<Vec<u8>, Header>;

#[derive(Clone)]
pub struct Header {
    file_id: u64,
    val_size: u32,
    val_offset: u32,
    ts_tamp: u32,
}


impl Debug for Header {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Header<fid={}, vsz={}, offset={}, ts={}>", self.file_id, self.val_size, self.val_offset, self.ts_tamp)
    }
}

/*
struct Entry {
    pub crc: Vec<u8>,
    pub ts_tamp: u32,
    pub key: Vec<u8>,
    pub val: Vec<u8>,
}

impl Debug for Entry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "Entry<key={}, val={}>", str::from_utf8(&self.key).unwrap(), str::from_utf8(&self.val).unwrap())
    }
}
*/


pub trait Reader {
    fn get(&mut self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>>;
}

pub trait Writer {
    fn put(&mut self, key: &[u8], val: &[u8]) -> anyhow::Result<()>;
    fn delete(&mut self, key: &[u8]) -> anyhow::Result<()>;
}

pub trait FsBackend {
    fn open(conf: Config) -> anyhow::Result<Self> where Self: Sized;
    fn new_active_file(&mut self) -> anyhow::Result<()>;
    fn sync(&mut self) -> anyhow::Result<()>;
}

pub trait FsReader {
    fn read_from_file(&mut self, file_id: u64, offset: u32, size: u32) -> anyhow::Result<Vec<u8>>;
}

pub trait FsWriter {
    fn write_to_file(&mut self, buf: &[u8]) -> anyhow::Result<()>;
}