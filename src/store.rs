use std::collections::HashMap;
use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};
use anyhow::{anyhow, Context, Result};
use bytes::{BufMut, BytesMut};
use crate::error::BitcaskError;


#[derive(Debug)]
struct Header {
    file_id: u32,
    val_size: u32,
    val_offset: u32,
    ts_tamp: u32,
}

const KEY_SIZE_OFFSET: usize = 0;
const VAL_SIZE_OFFSET: usize = 4;

struct Data {
    // crc: u8
    // ts_tamp: u32,
    key_size: u32,
    val_size: u32,
    key: Vec<u8>,
    val: Vec<u8>,
}

#[derive(Debug)]
pub struct Bitcask {
    active_file: fs::File,
    position: u64,
    // current position
    key_dir: HashMap<Vec<u8>, Header>,
}

impl Bitcask {
    pub fn start(dir: &str) -> Result<Bitcask> {
        fs::create_dir_all(dir).context("key dir creation failed")?;

        let path = Path::new(dir).join(format!("{}.bitcask.data", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis()));
        if let Ok(_) = fs::metadata(&path) {
            return Err(anyhow!("not implemented yet"));
        }

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;

        let bitcask = Bitcask {
            active_file: file,
            position: 0,
            key_dir: Default::default(),
        };

        Ok(bitcask)
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.active_file.seek(SeekFrom::Start(self.position))?;
        let mut payload = BytesMut::with_capacity( 4+4+ key.len() + val.len()); // key_size + val_size + key+val
        // TODO: BytesMut yerine direk [u8;1+1+kLen+vLen] kullanabilir miyiz bakalim.
        payload.put_u32(key.len() as u32);
        payload.put_u32(val.len() as u32);
        payload.put(key);
        payload.put(val);

        self.active_file.write_all(&payload).context("file write failed")?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::io::{Read, Seek, SeekFrom};
    use bytes::BytesMut;
    use tempdir::TempDir;
    use crate::store::Bitcask;

    #[test]
    fn it_should_create() {
        let dir = TempDir::new("bitcask-").unwrap();
        let cask_result = Bitcask::start(dir.path().to_str().unwrap());

        assert!(cask_result.is_ok());
        assert_eq!(0, cask_result.unwrap().position);
    }

    #[test]
    fn it_should_put() {
        let dir = TempDir::new("bitcask-").unwrap();
        let mut cask = Bitcask::start(dir.path().to_str().unwrap()).unwrap();
        let key = b"foo";
        let val = b"bar";
        cask.put(key, val);
        let mut payload = vec![0; 4+4+key.len()+val.len()]; // ksz + vsz + k + v

        cask.active_file.seek(SeekFrom::Start(0)).unwrap();
        cask.active_file.read_exact(&mut payload).unwrap();

        assert_eq!(u32::from_be_bytes(payload[0..4].try_into().unwrap()), key.len() as u32);;
        assert_eq!(u32::from_be_bytes(payload[4..8].try_into().unwrap()), val.len() as u32);;
        assert_eq!(payload[8..11], *key.as_slice());;
        assert_eq!(payload[11..14], *val.as_slice());;
    }
}