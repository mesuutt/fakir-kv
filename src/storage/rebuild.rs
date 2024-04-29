use std::collections::BTreeSet;
use std::ffi::OsStr;
use std::fs;
use std::path::Path;

use crate::storage::{Header, KeyDir};
use crate::storage::log::LogIterator;
use crate::storage::utils::{build_data_file_name, open_file_for_read};

pub fn rebuild_storage<P>(path: P) -> anyhow::Result<KeyDir> where P: AsRef<Path> {
    let mut key_dir = KeyDir::new();
    let file_ids = extract_data_file_ids(&path)?;
    for file_id in file_ids {
        load_from_data_file(&path, file_id, &mut key_dir)?;
    }
    Ok(key_dir)
}

fn load_from_data_file<P>(path: P, file_id: u64, key_dir: &mut KeyDir) -> anyhow::Result<()>
    where P: AsRef<Path> {
    let file = open_file_for_read(path, &build_data_file_name(file_id))?;
    let mut it = LogIterator::new(file);
    loop {
        match it.next() {
            None => { return Ok(()); }
            Some(result) => {
                let entry = result?;
                // TODO: Iterator direk header donebilir, ekstra alloc yapmaya gerek kalmaz.
                // Biz byte array'den header ve LogEntry olusturan iki fonksiyon olusturalim.
                // byte array alip bunlardan donsunler. Her yerde read write yapiyoruz, logic daginik kalmasin.
                let header = Header {
                    file_id,
                    val_size: entry.val_size,
                    val_offset: entry.val_offset,
                    ts_tamp: entry.ts_tamp,
                };

                // println!("{:?}, {:?}", header,  std::str::from_utf8(&entry.key));
                key_dir.insert(entry.key, header);
            }
        }
    }

    Ok(())
}

fn extract_data_file_ids<P>(path: P) -> anyhow::Result<impl Iterator<Item=u64>> where P: AsRef<Path> {
    Ok(fs::read_dir(path)?
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.is_file() && p.extension() == Some(OsStr::new("data")))
        .filter_map(|p|
            p.file_stem()
                .and_then(OsStr::to_str)
                .and_then(|s| s.split(".").next())
                .map(str::parse::<u64>)
        )
        .filter_map(Result::ok)
        .collect::<BTreeSet<u64>>()
        .into_iter()
    )
}