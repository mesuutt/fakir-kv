#[derive(Debug)]
pub struct Opts {
    pub expiry_secs: u32,
    pub sync_on_put: bool,
    pub max_file_size: u32,
}

impl Default for Opts {
    fn default() -> Self {
        Self {
            expiry_secs: 0,
            sync_on_put: false,
            max_file_size: 1 << 20, // 1MB
        }
    }
}

pub trait Reader {
    fn get(&mut self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>>;
}

pub trait Writer {
    fn put(&mut self, key: &[u8], val: &[u8]) -> anyhow::Result<()>;
    fn delete(&mut self, key: &[u8]) -> anyhow::Result<()>;
}