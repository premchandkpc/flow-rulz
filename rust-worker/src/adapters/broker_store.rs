//! Optional atomic snapshots for retained broker topics and consumer offsets.
use crate::ports::broker::Store;
use anyhow::{Result, bail};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;

pub(crate) struct DiskStore {
    root: PathBuf,
    _lock: File,
}

impl DiskStore {
    pub fn open(path: &Path) -> Result<Self> {
        fs::create_dir_all(path)?;
        let lock = super::file_log::acquire_process_lock_at(path, "broker.lock")?;
        Ok(Self {
            root: path.into(),
            _lock: lock,
        })
    }
}

impl Store for DiskStore {
    fn load(&self) -> Result<Option<Vec<u8>>> {
        let file = match File::open(self.root.join("broker.json")) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        let mut data = Vec::new();
        file.take((128 << 20) + 1).read_to_end(&mut data)?;
        if data.len() > 128 << 20 {
            bail!("broker snapshot exceeds 128 MiB");
        }
        Ok(Some(data))
    }

    fn save(&self, data: &[u8]) -> Result<()> {
        if data.len() > 128 << 20 {
            bail!("broker snapshot exceeds 128 MiB");
        }
        let path = self.root.join(format!("broker-{}.tmp", Uuid::new_v4()));
        let result = (|| {
            let mut options = OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options.open(&path)?;
            file.write_all(data)?;
            file.sync_all()?;
            drop(file);
            fs::rename(&path, self.root.join("broker.json"))?;
            File::open(&self.root)?.sync_all()?;
            Ok(())
        })();
        if let Err(error) = fs::remove_file(path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            return match result {
                Err(original) => Err(anyhow::anyhow!(
                    "snapshot write failed ({original:#}); temporary-file cleanup failed ({error})"
                )),
                Ok(()) => Err(error.into()),
            };
        }
        result
    }
}
