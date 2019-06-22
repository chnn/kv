use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io;
use std::io::prelude::*;
use std::io::{BufReader, SeekFrom};
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use serde_json;

#[derive(Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Rm { key: String },
}

impl fmt::Debug for Command {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", serde_json::to_string_pretty(&self).unwrap())
    }
}

#[derive(Debug)]
pub enum KvError {
    IoError(io::Error),
    JsonError(serde_json::Error),
    NotImplemented,
}

impl From<io::Error> for KvError {
    fn from(error: io::Error) -> Self {
        KvError::IoError(error)
    }
}

impl From<serde_json::Error> for KvError {
    fn from(error: serde_json::Error) -> Self {
        KvError::JsonError(error)
    }
}

pub type KvResult<T> = Result<T, KvError>;

pub struct KvStore {
    log: File,
    log_length: usize,
    index: HashMap<String, (u64, u64)>,
    pub compaction_threshold: usize,
}

impl KvStore {
    pub fn open(dir_path: impl Into<PathBuf>) -> KvResult<KvStore> {
        let dir_path = dir_path.into();
        fs::create_dir_all(&dir_path)?;

        let mut path = dir_path.clone();
        path.push("log");
        let log = OpenOptions::new()
            .read(true)
            .create(true)
            .append(true)
            .open(&path)?;

        let (index, log_length) = KvStore::build_index(&log)?;

        Ok(KvStore {
            log,
            index,
            log_length,
            compaction_threshold: 100,
        })
    }

    pub fn set(&mut self, key: String, value: String) -> KvResult<()> {
        let offsets = self.append_command(Command::Set {
            key: key.clone(),
            value,
        })?;

        self.index.insert(key, offsets);

        if self.log_length >= self.compaction_threshold {
            self.compact()?;
        }

        Ok(())
    }

    pub fn get(&mut self, key: String) -> KvResult<Option<String>> {
        match self.index.get(&key) {
            None => Ok(None),
            Some((i0, i1)) => {
                let length = (i1 - i0) as usize;
                let mut buf = vec![0u8; length];

                self.log.seek(SeekFrom::Start(*i0))?;
                self.log.read_exact(&mut buf)?;

                match serde_json::from_slice(&buf)? {
                    Command::Set { value, .. } => Ok(Some(value.to_string())),
                    Command::Rm { .. } => Ok(None),
                }
            }
        }
    }

    pub fn remove(&mut self, key: String) -> KvResult<()> {
        self.index.remove(&key);
        self.append_command(Command::Rm { key })?;

        Ok(())
    }

    fn append_command(&mut self, command: Command) -> KvResult<(u64, u64)> {
        let json = serde_json::to_string(&command)?;
        let start_offset = self.log.seek(SeekFrom::End(0))?;

        writeln!(&self.log, "{}", json)?;

        self.log_length += 1;

        let end_offset = self.log.seek(SeekFrom::End(0))?;

        Ok((start_offset, end_offset))
    }

    fn build_index(log: &File) -> KvResult<(HashMap<String, (u64, u64)>, usize)> {
        let f = BufReader::new(log);
        let mut index: HashMap<String, (u64, u64)> = HashMap::new();
        let mut i1: usize = 0;
        let mut log_length: usize = 0;

        for maybe_line in f.lines() {
            let line = maybe_line?;
            let i0 = i1;

            i1 += line.len();

            match serde_json::from_str(&line)? {
                Command::Set { key, .. } => index.insert(key.to_owned(), (i0 as u64, i1 as u64)),
                Command::Rm { key } => index.remove(&key),
            };

            log_length += 1;
        }

        Ok((index, log_length))
    }

    fn compact(&mut self) -> KvResult<()> {
        let compacted_len: u64 = self.index.values().map(|(i0, i1)| i1 - i0).sum();
        let mut compacted_log = vec![0u8; compacted_len as usize];
        let mut i = 0;

        for (i0, i1) in self.index.values() {
            let command_len = (i1 - i0) as usize;

            self.log.seek(SeekFrom::Start(*i0))?;
            self.log
                .read_exact(&mut compacted_log[i..(i + command_len)])?;

            i += command_len;
        }

        self.log.seek(SeekFrom::Start(0))?;
        self.log.set_len(compacted_len)?;
        self.log.write(&compacted_log)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn single_write_then_read() {
        let temp_dir = TempDir::new().unwrap();
        let mut kv = KvStore::open(temp_dir.path()).unwrap();

        kv.set("k1".to_owned(), "v1".to_owned()).unwrap();

        assert_eq!(kv.get("k1".to_owned()).unwrap().unwrap(), "v1".to_owned());
    }

    #[test]
    fn multiple_write_then_read() {
        let temp_dir = TempDir::new().unwrap();
        let mut kv = KvStore::open(temp_dir.path()).unwrap();

        kv.set("k1".to_owned(), "v1".to_owned()).unwrap();
        kv.set("k2".to_owned(), "v2".to_owned()).unwrap();
        kv.set("k3".to_owned(), "v3".to_owned()).unwrap();

        assert_eq!(kv.get("k1".to_owned()).unwrap().unwrap(), "v1".to_owned());
        assert_eq!(kv.get("k2".to_owned()).unwrap().unwrap(), "v2".to_owned());
        assert_eq!(kv.get("k3".to_owned()).unwrap().unwrap(), "v3".to_owned());
    }

    #[test]
    fn overwrite_then_read() {
        let temp_dir = TempDir::new().unwrap();
        let mut kv = KvStore::open(temp_dir.path()).unwrap();

        kv.set("k1".to_owned(), "v1".to_owned()).unwrap();
        kv.set("k2".to_owned(), "v2".to_owned()).unwrap();
        kv.set("k1".to_owned(), "v3".to_owned()).unwrap();

        assert_eq!(kv.get("k1".to_owned()).unwrap().unwrap(), "v3".to_owned());
        assert_eq!(kv.get("k2".to_owned()).unwrap().unwrap(), "v2".to_owned());
    }

    #[test]
    fn write_then_remove() {
        let temp_dir = TempDir::new().unwrap();
        let mut kv = KvStore::open(temp_dir.path()).unwrap();

        kv.set("k1".to_owned(), "v1".to_owned()).unwrap();
        kv.remove("k1".to_owned()).unwrap();

        assert_eq!(kv.get("k1".to_owned()).unwrap(), None);
    }

    #[test]
    fn remove_non_existant() {
        let temp_dir = TempDir::new().unwrap();
        let mut kv = KvStore::open(temp_dir.path()).unwrap();

        kv.remove("k1".to_owned()).unwrap();

        assert_eq!(kv.get("k1".to_owned()).unwrap(), None);
    }

    #[test]
    fn compaction_reduces_logsize() {
        let temp_dir = TempDir::new().unwrap();
        let mut kv = KvStore::open(temp_dir.path()).unwrap();

        kv.compaction_threshold = 5;

        kv.set("k1".to_owned(), "v1".to_owned()).unwrap();
        kv.set("k1".to_owned(), "v2".to_owned()).unwrap();
        kv.set("k1".to_owned(), "v3".to_owned()).unwrap();
        kv.set("k1".to_owned(), "v4".to_owned()).unwrap();

        let mut log_a = String::new();

        kv.log.seek(SeekFrom::Start(0)).unwrap();
        kv.log.read_to_string(&mut log_a).unwrap();

        kv.set("k1".to_owned(), "v5".to_owned()).unwrap();

        let mut log_b = String::new();

        kv.log.seek(SeekFrom::Start(0)).unwrap();
        kv.log.read_to_string(&mut log_b).unwrap();

        assert!(log_b.len() < log_a.len());
    }
}
