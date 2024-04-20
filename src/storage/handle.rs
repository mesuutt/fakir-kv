use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::sync::Arc;

use anyhow::Context;

use crate::storage::{file_lock, Reader, utils};
use crate::storage::config::Config;
use crate::storage::context::{ReadContext, WriteContext};
use crate::storage::log_reader::LogReader;
use crate::storage::log_writer::LogWriter;

pub struct Handle<'a> {
    read_ctx: ReadContext,
    write_ctx: &'a WriteContext,
    conf: &'a Config,
    writer: LogWriter<'a>,

    /**
    We use RefCell because we are update readers if read ops comes for new active_file after startup
     */
    readers: RefCell<HashMap<u64, LogReader>>,
}

impl<'a> Handle<'a> {
    pub fn open(conf: &'a Config, write_ctx: &'a WriteContext) -> anyhow::Result<Handle<'a>> {
        // TODO: rebuild storage
        fs::create_dir_all(&conf.path).context("data directory creation failed")?;
        file_lock::try_lock_db(&conf.path)?;

        let read_ctx = ReadContext::new(conf.clone());
        let writer = LogWriter::new(&write_ctx).unwrap();

        let mut readers = HashMap::new();
        readers.insert(writer.file_id(), LogReader::new(&read_ctx.conf.path, writer.file_id())?);

        Ok(Handle {
            read_ctx,
            write_ctx,
            writer,
            conf,
            readers: RefCell::new(readers),
        })
    }

    pub fn get(&mut self, key: &[u8]) -> anyhow::Result<Option<Vec<u8>>> {
        let bind = self.write_ctx.key_dir.read().unwrap();
        let h = match bind.get(key) {
            None => { return Ok(None); }
            Some(h) => {
                if h.ts_tamp < utils::expiry_time(self.write_ctx.conf.expiry_secs) {
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

