use std::path::PathBuf;

#[derive(Debug, Clone)]
pub struct Config {
    pub path: PathBuf,
    pub expiry_secs: u32,
    pub sync_on_put: bool,
    pub max_file_size: u32,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            path: PathBuf::new(),
            expiry_secs: 0,
            sync_on_put: false,
            max_file_size: 1 << 20, // 1MB
        }
    }
}
