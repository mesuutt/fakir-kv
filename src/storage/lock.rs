use std::{fs, process};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::thread::sleep;
use std::time::Duration;

use anyhow::{anyhow, Context};
use fs2::FileExt;

pub(crate) fn try_lock_db(dir: &str) -> anyhow::Result<()> {
    let path = Path::new(dir).join("pid.lock");
    let file = OpenOptions::new()
        .create_new(true)// return err if file already exists
        .write(true)
        .open(&path);

    match file {
        Ok(mut f) => {
            write_pid(&mut f)?
        }
        Err(_) => {
            let mut file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(&path)?;

            file.try_lock_exclusive().context("process already running")?;

            let mut pid = String::new();
            file.read_to_string(&mut pid)?;

            if pid == "" {
                return Err(anyhow!(format!("cannot read PID from lock file({}). You can remove lock file after ensure server is not runnung.", path.clone().display())));
            }

            unsafe {
                if nix::libc::kill(pid.parse()?, 0) == 0 {
                    return Err(anyhow!("process already running"));
                }
            }

            println!("sleeping in lock");
            sleep(Duration::from_secs(100*100));
            write_pid(&mut file)?
        }
    }

    Ok(())
}


fn write_pid(file: &mut fs::File) -> anyhow::Result<()> {
    file.seek(SeekFrom::Start(0)).context("lock file seek failed")?;
    let pid = process::id().to_string();
    file.write_all(pid.as_bytes())?;
    file.sync_all()?;
    Ok(())
}
