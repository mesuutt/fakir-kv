use std::cell::{Ref, RefCell};
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use anyhow::Context;

use crate::storage::{file_lock, Reader, utils};
use crate::storage::config::Config;
use crate::storage::context::AppContext;
use crate::storage::log_reader::LogReader;
use crate::storage::log_writer::LogWriter;

pub struct Handle {
    ctx: Arc<AppContext>,
    // TODO: RwLock yaptim ama bilemedim gereklimi.
    writer: LogWriter,

    /**
    We use RefCell because we are update readers if read ops comes for new active_file after startup
    */
    readers: RefCell<HashMap<u64, LogReader>>,
}

impl Handle {
    pub fn open(conf: Config) -> anyhow::Result<Self> {
        // TODO: rebuild storage
        fs::create_dir_all(&conf.path).context("data directory creation failed")?;
        file_lock::try_lock_db(&conf.path)?;

        let ctx = Arc::new(AppContext::new(conf, Default::default()));
        let writer = LogWriter::new(ctx.clone()).unwrap();

        let mut readers = HashMap::new();
        readers.insert(writer.file_id(), LogReader::new(&ctx.conf.path, writer.file_id())?);

        Ok(Handle {
            ctx,
            writer,
            readers: RefCell::new(readers),
        })
    }

    pub fn get(&mut self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let bind = self.ctx.key_dir.read().unwrap();
        let h = match bind.get(key) {
            None => { return Ok(None) }
            Some(h) => {
                if h.ts_tamp < utils::expiry_time(self.ctx.conf.expiry_secs) {
                    // write causes to DEADLOCK
                    // self.writer.delete(key)?;
                    return Ok(None);
                }
                h
            }
        };

        Ok(Some(self.read(h.file_id, h.val_offset, h.val_size)?))
    }

    fn read(&self, file_id: u64, offset: u32, size: u32) -> anyhow::Result<Vec<u8>> {
        let mut readers = self.readers.borrow_mut();
        if let None = readers.get(&file_id) {
            let r = LogReader::new(&self.ctx.conf.path, file_id)?;
            readers.insert(file_id, r);
        };

        let reader = readers.get(&file_id).unwrap();
        reader.read(offset, size)
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) -> anyhow::Result<()> {
        self.writer.write(key, val)
    }

    pub fn delete(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.writer.delete(key)
    }
}

