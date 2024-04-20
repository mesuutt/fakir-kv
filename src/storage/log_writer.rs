use std::fs;
use std::io::{stderr, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Context;
use bytes::BufMut;

use crate::storage::{CRC_OFFSET, CRC_SIZE, Header, KEY_OFFSET, KEY_SIZE_OFFSET, TOMBSTONE_MARKER_CHAR, utils, VAL_SIZE_OFFSET};
use crate::storage::context::{ReadContext, WriteContext};
use crate::storage::utils::{build_data_file_name, open_file_for_write};

pub struct LogWriter<'a> {
    file_id: u64,
    file: fs::File,
    position: u32,
    ctx: &'a WriteContext,
}

impl<'a> LogWriter<'a> {
    pub fn new(ctx: &'a WriteContext) -> anyhow::Result<Self> {
        let file_id = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let file = open_file_for_write(&ctx.conf.path, &build_data_file_name(file_id))?;

        Ok(LogWriter { file_id, file, ctx, position: 0 })
    }

    pub fn file_id(&self) -> u64 {
        self.file_id
    }

    pub fn write(&mut self, key: &[u8], val: &[u8]) -> anyhow::Result<()> {
        /*
        dbg!(CRC_SIZE);
        dbg!(TS_SIZE);
        dbg!(KEY_SIZE);
        dbg!(KEY_SIZE_OFFSET, KEY_OFFSET);
        dbg!(VAL_SIZE, VAL_SIZE_OFFSET);
        dbg!(CRC_SIZE + TS_SIZE + KEY_SIZE + VAL_SIZE);
        */

        let ts_tamp = utils::timestamp();
        let entry_bytes = create_entry(key, val, ts_tamp);
        let entry_start_pos = self.position;

        self.write_to_file(&entry_bytes)?;
        self.sync()?;

        let header = Header {
            file_id: self.file_id,
            val_size: val.len() as u32,
            val_offset: entry_start_pos + (KEY_OFFSET + key.len()) as u32,
            ts_tamp,
        };

        if key == &[107, 95, 50] {
            println!("{:?}", header);
            debug_entry(&entry_bytes);
        }

        println!("{:?}", header);
        // debug_entry(&entry_bytes);

        self.ctx.key_dir.try_write().unwrap().insert(key.to_vec(), header);

        if self.position > self.ctx.conf.max_file_size {
            self.new_active_file()?;
        }

        Ok(())
    }

    fn new_active_file(&mut self) -> anyhow::Result<()> {
        let new_file_id = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let new_filename = build_data_file_name(new_file_id);

        self.file.sync_all()?;
        self.file = open_file_for_write(&self.ctx.conf.path, &new_filename)?;
        ;
        self.file_id = new_file_id;
        self.position = 0;

        Ok(())
    }

    pub fn delete(&mut self, key: &[u8]) -> anyhow::Result<()> {
        self.write(key, &[TOMBSTONE_MARKER_CHAR; 1]).context("key deletion failed")?;
        Ok(())
    }

    #[inline]
    fn sync(&mut self) -> anyhow::Result<()> {
        // TODO: we can create flush_on_put config for flushing after puts.
        // flushing content can be not needed for some use cases?
        self.file.flush()?;
        if self.ctx.conf.sync_on_put {
            self.file.sync_data()?;
        }
        Ok(())
    }

    fn write_to_file(&mut self, buf: &[u8]) -> anyhow::Result<()> {
        self.file.write_all(buf).context("file write failed")?;
        self.position += buf.len() as u32;

        Ok(())
    }
}


impl Drop for LogWriter<'_> {
    fn drop(&mut self) {
        if let Err(e) = self.file.sync_all() {
            write!(stderr(), "error while closing active file: {:?}", e).expect("error writing to stderr");
        }
    }
}

fn create_entry(key: &[u8], val: &[u8], ts_tamp: u32) -> Vec<u8> {
    let mut payload = Vec::with_capacity(KEY_OFFSET + key.len() + val.len());

    payload.put_u32(0); // empty space for crc
    payload.put_u32(ts_tamp);
    payload.put_u32(key.len() as u32);
    payload.put_u32(val.len() as u32);
    payload.put(key);
    payload.put(val);

    let checksum = crc32fast::hash(&payload[CRC_OFFSET + CRC_SIZE..]);
    payload.splice(0..CRC_SIZE, checksum.to_be_bytes());

    payload
}


fn debug_entry(payload: &[u8]) {
    let key_size = u32::from_be_bytes(payload[KEY_SIZE_OFFSET..VAL_SIZE_OFFSET].try_into().unwrap()) as usize;
    let val_size = u32::from_be_bytes(payload[VAL_SIZE_OFFSET..KEY_OFFSET].try_into().unwrap()) as usize;

    let key = &payload[KEY_OFFSET..KEY_OFFSET + key_size];

    let val_offset = KEY_OFFSET + key_size;
    let val = &payload[val_offset..val_offset + val_size];
    if val == &[8] {
        println!("DeleteEntry<key={}>", std::str::from_utf8(&key).unwrap())
    } else {
        println!("PutEntry<key={}, val={}>", std::str::from_utf8(&key).unwrap(), std::str::from_utf8(&val).unwrap())
    }
}
