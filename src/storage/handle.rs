use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::rc::Rc;
use std::sync::{Arc, RwLock};

use anyhow::Context;

use crate::storage::{file_lock, Header, KeyDir, Reader, utils};
use crate::storage::config::Config;
use crate::storage::context::{ReadContext, WriteContext};
use crate::storage::log_reader::LogReader;
use crate::storage::log_writer::LogWriter;

pub struct Handle<'a> {
    conf: &'a Config,
    writer: LogWriter<'a>,
    key_dir: Arc<RwLock<KeyDir>>,
    /**
    We use RefCell because we are update readers if read ops comes for new active_file after startup
     */
    readers: RefCell<HashMap<u64, LogReader>>,
}

impl<'a> Handle<'a> {
    pub fn open(conf: &'a Config) -> anyhow::Result<Handle<'a>> {
        // TODO: rebuild storage
        fs::create_dir_all(&conf.path).context("data directory creation failed")?;
        file_lock::try_lock_db(&conf.path)?;

        let key_dir = Arc::new(RwLock::new(Default::default()));
        let writer = LogWriter::new(&conf, key_dir.clone()).unwrap();

        let mut readers = HashMap::new();
        readers.insert(writer.file_id(), LogReader::new(&conf.path, writer.file_id())?);

        Ok(Handle {
            key_dir,
            writer,
            conf,
            readers: RefCell::new(readers),
        })
    }

    pub fn get(&mut self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let expired = {
            let bind = self.key_dir.read().unwrap();
            let header = bind.get(key);
            match header {
                None => { return Ok(None); }
                Some(header) => {
                    if header.ts_tamp > utils::expiry_time(self.conf.expiry_secs) {
                        return Ok(Some(self.read(header.file_id, header.val_offset, header.val_size)?))
                    }
                    true
                }
            }
        };

        if expired {
            self.writer.delete(key)?;
            return Ok(None);
        }

        unreachable!()
    }

    fn read(&self, file_id: u64, offset: u32, size: u32) -> anyhow::Result<Vec<u8>> {
        let mut readers = self.readers.borrow_mut();
        if let None = readers.get(&file_id) {
            let r = LogReader::new(&self.conf.path, file_id)?;
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

