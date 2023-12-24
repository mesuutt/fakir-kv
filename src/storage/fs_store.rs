use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, stderr, Write};
use std::os::fd::AsFd;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use bytes::BufMut;
use fs2::FileExt;
use crate::cask::Writer;

use crate::cask::{Reader, FsBackend, Opts};
use crate::storage::{CRC_OFFSET, CRC_SIZE, file_lock, Header, KEY_OFFSET, KeyDir};

#[derive(Debug)]
pub struct FsStorage {
    active_file: fs::File,
    active_file_id: u64,
    position: u32,
    key_dir: KeyDir,
    dir: PathBuf,
    read_file: fs::File,
    ops: Opts,
}

impl FsStorage {
    fn read_val(&mut self, file_id: u64, offset: u32, size: u32) -> Result<Vec<u8>> {
        self.read_file.seek(SeekFrom::Start(offset as u64))?;

        let mut buf = vec![0u8; size as usize];
        self.read_file.read_exact(&mut buf)?;

        Ok(buf)
    }

    fn write(&mut self, buf: &[u8]) -> Result<()> {
        // self.active_file.seek(SeekFrom::Start(self.position as u64))?;
        self.active_file.write_all(&buf).context("file write failed")?;
        self.position += buf.len() as u32;

        Ok(())
    }

    #[inline]
    fn make_filename(file_id: u64) -> String {
        format!("{}.bitcask.data", file_id)
    }
}

impl Reader for FsStorage {
    fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>> where Self: Reader {
        match self.key_dir.get(key) {
            None => { Ok(None) }
            Some(h) => {
                if h.ts_tamp < expiry_time(self.ops.expiry_secs) {
                    self.key_dir.remove(key);
                    return Ok(None);
                }

                Ok(Some(self.read_val(h.file_id, h.val_offset, h.val_size)?))
            }
        }
    }
}

impl Writer for FsStorage {
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        /*
        dbg!(CRC_SIZE);
        dbg!(TS_SIZE);
        dbg!(KEY_SIZE);
        dbg!(KEY_SIZE_OFFSET, KEY_OFFSET);
        dbg!(VAL_SIZE, VAL_SIZE_OFFSET);
        dbg!(CRC_SIZE + TS_SIZE + KEY_SIZE + VAL_SIZE);
        */

        let ts_tamp = current_timestamp();
        let entry_bytes = create_entry(key, val, ts_tamp);
        let entry_start_pos = self.position;

        self.write(&entry_bytes)?;
        self.sync()?;

        self.key_dir.insert(key.to_vec(), Header {
            file_id: self.active_file_id,
            val_size: val.len() as u32,
            val_offset: entry_start_pos + (KEY_OFFSET + key.len()) as u32,
            ts_tamp,
        });

        Ok(())
    }
}

impl FsBackend for FsStorage {
    fn open(dir: &str, ops: Opts) -> Result<Self> {
        fs::create_dir_all(dir).context("data directory creation failed")?;
        file_lock::try_lock_db(dir)?;

        let active_file_id = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let path = Path::new(dir).join(FsStorage::make_filename(active_file_id));
        if let Ok(_) = fs::metadata(&path) {
            return Err(anyhow!("not implemented yet"));
        }

        let active_file = OpenOptions::new()
            .append(true)
            // .read(true)
            .create(true)
            .open(&path)?;

        let read_file = OpenOptions::new()
            .read(true)
            .open(path)?;

        let bitcask = FsStorage {
            active_file,
            read_file,
            active_file_id,
            position: 0,
            key_dir: Default::default(),
            dir: dir.parse()?,
            ops,
        };

        Ok(bitcask)
    }

    #[inline]
    fn sync(&mut self) -> Result<()> {
        if self.ops.sync_on_put {
            self.active_file.sync_data()?;
        }
        Ok(())
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

fn expiry_time(expire_secs: u32) -> u32 {
    if expire_secs > 0 {
        current_timestamp() - expire_secs
    } else {
        0
    }
}

#[inline]
fn current_timestamp() -> u32 {
    return SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs() as u32;
}

impl Drop for FsStorage {
    fn drop(&mut self) {
        if let Err(e) = self.active_file.sync_all() {
            write!(stderr(), "error while closing active file: {:?}", e).expect("error writing to stderr");
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs::File;
    use std::io::{Read, Seek, SeekFrom};
    use std::path::Path;

    use tempdir::TempDir;
    use crate::cask::{FsBackend, Reader, Writer};

    use crate::storage::{CRC_OFFSET, CRC_SIZE, KEY_OFFSET, KEY_SIZE_OFFSET, VAL_SIZE_OFFSET};

    use super::FsStorage;

    #[test]
    fn it_should_load_cask_from_file() {
        let dir = TempDir::new("bitcask-").unwrap();
        let cask_result = FsStorage::open(dir.path().to_str().unwrap(), Default::default());

        assert!(cask_result.is_ok());
        assert_eq!(0, cask_result.unwrap().position);
    }

    #[test]
    fn it_should_put_to_file() {
        // given
        let dir = TempDir::new("bitcask-").unwrap();
        let mut write_cask = FsStorage::open(dir.path().to_str().unwrap(), Default::default()).unwrap();
        let key = b"foo";
        let val = b"bar";

        // when
        write_cask.put(key, val).unwrap();

        // then
        let mut payload = vec![0; KEY_OFFSET + key.len() + val.len()];

        let mut cask_file = File::open(Path::join(dir.path(), FsStorage::make_filename(write_cask.active_file_id))).unwrap();
        cask_file.seek(SeekFrom::Start(0)).unwrap();
        cask_file.read_exact(&mut payload).unwrap();

        let payload_without_crc = payload[CRC_OFFSET + CRC_SIZE..].to_vec();
        let checksum = crc32fast::hash(&payload_without_crc);
        assert_eq!(u32::from_be_bytes(payload[CRC_OFFSET..CRC_OFFSET + CRC_SIZE].try_into().unwrap()), checksum);

        assert_eq!(u32::from_be_bytes(payload[KEY_SIZE_OFFSET..VAL_SIZE_OFFSET].try_into().unwrap()), key.len() as u32);
        assert_eq!(u32::from_be_bytes(payload[VAL_SIZE_OFFSET..KEY_OFFSET].try_into().unwrap()), val.len() as u32);

        assert_eq!(payload[KEY_OFFSET..KEY_OFFSET + key.len()], *key.as_slice());

        let val_offset = KEY_OFFSET + key.len();
        assert_eq!(payload[val_offset..val_offset + val.len()], *val.as_slice());

        let header = write_cask.key_dir.get(key.as_slice()).unwrap();
        assert_eq!(header.file_id, write_cask.active_file_id);
        assert_eq!(header.val_size, val.len() as u32);
        assert_eq!(header.val_offset, (KEY_OFFSET + key.len()).try_into().unwrap());
        assert_eq!(write_cask.position, (KEY_OFFSET + key.len() + val.len()).try_into().unwrap());
    }

    #[test]
    fn it_should_get() {
        // given
        let dir = TempDir::new("bitcask-").unwrap();
        let mut cask = FsStorage::open(dir.path().to_str().unwrap(), Default::default()).unwrap();

        let pairs: Vec<(&[u8], &[u8])> = vec![
            (b"key1", b"val1"),
            (b"key2", b"val2"),
            (b"key1", b"val3"),
        ];

        for (key, val) in pairs {
            cask.put(key, val).unwrap();

            let actual = cask.get(key).unwrap().unwrap();
            assert_eq!(val, actual.as_slice());
        }
    }
}