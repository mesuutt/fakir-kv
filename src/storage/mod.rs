use std::collections::HashMap;
use std::fmt::{Debug, Formatter};

pub use config::Config;
pub use handle::Handle;

mod file_lock;
mod utils;
mod log_reader;
mod handle;
mod config;
mod context;
mod log_writer;
mod rebuild;
mod log;

// TODO: We can benchmark BtreeMap: https://www.dotnetperls.com/btreemap-rust
type KeyDir = HashMap<Vec<u8>, Header>;

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