use std::{fs, process};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path};

use anyhow::{bail, Context};
use fs2::FileExt;
use log::debug;

pub(crate) fn try_lock_db<P>(dir: P) -> anyhow::Result<()> where P: AsRef<Path> {
    let pid_file_path = dir.as_ref().join("pid.lock");

    if let Ok(mut f) = OpenOptions::new()
        .create_new(true)// return err if file already exists
        .write(true)
        .open(&pid_file_path) {
        return write_pid(&mut f);
    }

    debug!("lock file exist.");

    let mut pid_file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&pid_file_path)?;

    pid_file.try_lock_exclusive().context("process already running")?;

    let mut pid = String::new();
    pid_file.read_to_string(&mut pid)?;

    if pid.is_empty() {
        return bail!(format!("cannot read PID from lock file({}). You can remove lock file after ensure server is not running.", pid_file_path.clone().display()));
    }

    unsafe {
        if libc::kill(pid.parse()?, 0) == 0 {
            return bail!("process already running");
        }
    }

    write_pid(&mut pid_file)
}


fn write_pid(file: &mut fs::File) -> anyhow::Result<()> {
    file.seek(SeekFrom::Start(0)).context("lock file seek failed")?;
    let pid = process::id().to_string();
    file.write_all(pid.as_bytes())?;
    file.sync_all()?;
    Ok(())
}
