use std::{fs, process};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use anyhow::{bail, Context};
use fs2::FileExt;
use log::debug;

pub(crate) fn try_lock_db(dir: &str) -> anyhow::Result<()> {
    let path = Path::new(dir).join("pid.lock");
    let file = OpenOptions::new()
        .create_new(true)// return err if file already exists
        .write(true)
        .open(&path);

    if let Ok(mut f) = file {
        return Ok(write_pid(&mut f)?);
    }

    debug!("lock file exist.");

    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)?;

    file.try_lock_exclusive().context("process already running")?;

    let mut pid = String::new();
    file.read_to_string(&mut pid)?;

    if pid == "" {
        return bail!(format!("cannot read PID from lock file({}). You can remove lock file after ensure server is not running.", path.clone().display()));
    }

    unsafe {
        if libc::kill(pid.parse()?, 0) == 0 {
            return bail!("process already running");
        }
    }

    Ok(write_pid(&mut file)?)
}


fn write_pid(file: &mut fs::File) -> anyhow::Result<()> {
    file.seek(SeekFrom::Start(0)).context("lock file seek failed")?;
    let pid = process::id().to_string();
    file.write_all(pid.as_bytes())?;
    file.sync_all()?;
    Ok(())
}
