// [crc|ts_tamp|ksz|vsz|key|val]

use std::{fs, io, str};
use std::fmt::{Debug, Formatter};
use std::io::{ErrorKind, Read, Seek};
use std::mem::size_of;

use anyhow::Error;

use crate::storage::Header;

pub const CRC_SIZE: usize = size_of::<u32>();
pub const TS_SIZE: usize = size_of::<u32>();
pub const KEY_SIZE: usize = size_of::<u32>();
pub const VAL_SIZE: usize = size_of::<u32>();

pub const CRC_OFFSET: usize = 0;
pub const KEY_SIZE_OFFSET: usize = CRC_SIZE + TS_SIZE;
pub const VAL_SIZE_OFFSET: usize = KEY_SIZE_OFFSET + KEY_SIZE;
pub const KEY_OFFSET: usize = VAL_SIZE_OFFSET + VAL_SIZE;

// use backspace char as tombstone marker
pub const TOMBSTONE_MARKER_CHAR: u8 = 8;

pub struct LogEntry {
    // pub crc: [u8; CRC_SIZE],
    pub crc: Vec<u8>,
    // TODO: [u8;4] gibi kullanabiliriz.
    pub ts_tamp: u32,
    pub val_size: u32,
    pub val_offset: u32,
    pub key: Vec<u8>,
    pub val: Vec<u8>,
}

impl Debug for LogEntry {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "LogEntry<key={}, val={}>", str::from_utf8(&self.key).unwrap(), str::from_utf8(&self.val).unwrap())
    }
}

pub struct LogIterator {
    file: fs::File,
    file_id: u64,
}

impl LogIterator {
    pub fn new(file_id: u64, file: fs::File) -> Self {
        Self { file_id, file }
    }

    fn read_to(&mut self, buf: &mut [u8]) -> Result<usize, io::Error> {
        match self.file.read(buf) {
            Ok(size) => Ok(size),
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => Ok(0),
            Err(e) => Err(e),
        }
    }
}

impl Iterator for LogIterator {
    type Item = anyhow::Result<(Vec<u8>, Header)>;

    fn next(&mut self) -> Option<Self::Item> {
        // TODO: crc check?
        let mut crc = [0u8; CRC_SIZE];
        let result = self.read_to(&mut crc);
        if let Err(e) = result {
            return Some(Err(Error::from(e)));
        }

        // CRC is first byte of the entry, so If 0 byte consumed, it means EOF
        if result.unwrap() == 0 {
            return None;
        }

        let mut timestamp = [0u8; TS_SIZE];
        let result = self.read_to(&mut timestamp);
        if let Err(e) = result {
            return Some(Err(Error::from(e)));
        }

        let mut key_size_bytes = [0u8; KEY_SIZE];
        let result = self.read_to(&mut key_size_bytes);
        if let Err(e) = result {
            return Some(Err(Error::from(e)));
        }

        let key_size = u32::from_be_bytes(key_size_bytes);

        let mut val_size_bytes = [0u8; VAL_SIZE];
        let result = self.read_to(&mut val_size_bytes);
        if let Err(e) = result {
            return Some(Err(Error::from(e)));
        }

        let val_size = u32::from_be_bytes(val_size_bytes);

        let mut key = vec![0u8; key_size as usize];

        let result = self.read_to(&mut key);
        if let Err(e) = result {
            return Some(Err(Error::from(e)));
        }

        let stream_pos = match self.file.stream_position() {
            Ok(pos) => { pos }
            Err(e) => return Some(Err(Error::from(e))),
        };

        let val_offset = match u32::try_from(stream_pos) {
            Ok(x) => { x }
            Err(e) => {
                return Some(Err(Error::from(e)));
            }
        };

        let mut val = vec![0u8; val_size as usize];
        let result = self.read_to(&mut val);
        if let Err(e) = result {
            return Some(Err(Error::from(e)));
        }

        Some(Ok((key, Header {
            file_id: self.file_id,
            ts_tamp: u32::from_be_bytes(timestamp),
            val_size,
            val_offset,
        })))
    }
}

fn byte_to_header(buf: &[u8]) -> anyhow::Result<Header> {
    todo!()
}

fn byte_to_entry(buf: &[u8]) -> anyhow::Result<Header> {
    todo!()
}
