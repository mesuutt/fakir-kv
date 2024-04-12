use std::sync::RwLock;

use crossbeam::atomic::AtomicCell;

use crate::storage::config::Config;
use crate::storage::KeyDir;

pub struct AppContext {
    pub key_dir: RwLock<KeyDir>,
    pub conf: Config,
    pub closed: AtomicCell<bool>,
}

impl AppContext {
    pub fn new(conf: Config, key_dir: KeyDir) -> Self {
        Self {
            conf,
            key_dir: RwLock::new(key_dir),
            closed: AtomicCell::new(false),
        }
    }
}
