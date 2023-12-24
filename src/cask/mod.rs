#[derive(Debug, Default)]
pub struct Opts {
    pub expiry_secs: u32,
    pub sync_on_put: bool,
}

pub trait Reader {
    fn get(&mut self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>>;
}

pub trait Writer {
    fn put(&mut self, key: &[u8], val: &[u8]) -> anyhow::Result<()>;
}

pub trait FsBackend {
    fn open(dir: &str, options: Opts) -> anyhow::Result<Self> where Self: Sized;
    fn sync(&mut self) -> anyhow::Result<()>;
}