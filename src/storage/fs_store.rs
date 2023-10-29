use std::fs;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context, Result};
use bytes::{BufMut, BytesMut};

use crate::error::BitcaskError;
use crate::storage::{Header, KEY_SIZE, KeyDir, Storage, VAL_SIZE};

#[derive(Debug)]
pub struct FsStorage {
    active_file: fs::File,
    active_file_id: u64,
    position: u32,
    // current position
    key_dir: KeyDir,
}

impl FsStorage {
    pub fn load(dir: &str) -> Result<Self> {
        fs::create_dir_all(dir).context("key dir creation failed")?;

        let active_file_id = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        let path = Path::new(dir).join(format!("{}.bitcask.data", &active_file_id));
        if let Ok(_) = fs::metadata(&path) {
            return Err(anyhow!("not implemented yet"));
        }

        let file = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(path)?;

        let bitcask = FsStorage {
            active_file: file,
            active_file_id,
            position: 0,
            key_dir: Default::default(),
        };

        Ok(bitcask)
    }
}

impl Storage for FsStorage {
    fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        self.active_file.seek(SeekFrom::Start(self.position as u64))?;
        let mut payload = BytesMut::with_capacity(KEY_SIZE + VAL_SIZE + key.len() + val.len()); // key_size + val_size + key+val

        payload.put_u32(key.len() as u32);
        payload.put_u32(val.len() as u32);
        payload.put(key);
        payload.put(val);

        let ts_tamp = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs();
        self.active_file.write_all(&payload).context("file write failed")?;
        self.key_dir.insert(key.to_vec(), Header {
            file_id: self.active_file_id,
            val_size: val.len() as u32,
            val_offset: self.position + 4 + 4 + key.len() as u32,
            ts_tamp,
        });

        self.position += payload.len() as u32;

        Ok(())
    }

    fn get(&self, key: &[u8]) -> Result<&[u8]> {
        match self.key_dir.get(key) {
            None => { Err(BitcaskError::NotFound.into()) }
            Some(h) => {
                Ok(&[0u8; 1])
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::io::{Read, Seek, SeekFrom};

    use tempdir::TempDir;

    use crate::storage::{KEY_OFFSET, KEY_SIZE, KEY_SIZE_OFFSET, Storage, VAL_SIZE, VAL_SIZE_OFFSET};

    use super::FsStorage;

    #[test]
    fn it_should_load_cask_from_file() {
        let dir = TempDir::new("bitcask-").unwrap();
        let cask_result = FsStorage::load(dir.path().to_str().unwrap());

        assert!(cask_result.is_ok());
        assert_eq!(0, cask_result.unwrap().position);
    }

    #[test]
    fn it_should_put_to_file() {
        let dir = TempDir::new("bitcask-").unwrap();
        let mut cask = FsStorage::load(dir.path().to_str().unwrap()).unwrap();
        let key = b"foo";
        let val = b"bar";
        cask.put(key, val);
        let mut payload = vec![0; KEY_SIZE + VAL_SIZE + key.len() + val.len()]; // ksz + vsz + k + v

        cask.active_file.seek(SeekFrom::Start(0)).unwrap();
        cask.active_file.read_exact(&mut payload).unwrap();

        assert_eq!(u32::from_be_bytes(payload[KEY_SIZE_OFFSET..VAL_SIZE_OFFSET].try_into().unwrap()), key.len() as u32);
        assert_eq!(u32::from_be_bytes(payload[VAL_SIZE_OFFSET..KEY_OFFSET].try_into().unwrap()), val.len() as u32);
        let val_offset = KEY_OFFSET + key.len();
        assert_eq!(payload[KEY_OFFSET..KEY_OFFSET + key.len()], *key.as_slice());
        assert_eq!(payload[val_offset..val_offset + val.len()], *val.as_slice());
        dbg!(cask.key_dir.get(&key.to_vec()).unwrap());

        let header = cask.key_dir.get(key.as_slice()).unwrap();
        assert_eq!(header.file_id, cask.active_file_id);
        assert_eq!(header.val_size, val.len() as u32);
        assert_eq!(header.val_offset, (KEY_SIZE + VAL_SIZE + key.len()).try_into().unwrap());
        assert_eq!(cask.position, (KEY_SIZE + VAL_SIZE + key.len() + val.len()).try_into().unwrap());
    }
}