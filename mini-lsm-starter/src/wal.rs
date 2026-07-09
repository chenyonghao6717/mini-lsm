// REMOVE THIS LINE after fully implementing this functionality
// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use anyhow::{Result, anyhow};
use bytes::Bytes;
use crc32fast::Hasher;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use crate::block::{KEY_LEN_SIZE, VALUE_LEN_SIZE};
use crate::key::{KeyBytes, KeySlice, TS_SIZE};
use crate::table::CHECKSUM_SIZE;

const BODY_LEN_SIZE: usize = 4;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
    max_ts: AtomicU64,
}

/// Atomic WAL record layout
/// |------------------------------------|
/// | header u32 |   body  |  footer u32 |
/// |------------------------------------|
/// |  body len  | entries |   checksum  |
/// |------------------------------------|
/// |--------------------------------------------------------------------|
/// |                          entry                                     |
/// |--------------------------------------------------------------------|
/// | key_len(u16, exclude ts) | key | ts (u64) | value_len(u16) | value |
/// |--------------------------------------------------------------------|
impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path.as_ref())?;
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
            max_ts: AtomicU64::new(0),
        })
    }

    pub fn get_max_ts(&self) -> u64 {
        self.max_ts.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn read_next_record(buf: &[u8], start: usize) -> (KeyBytes, Bytes) {
        let key_len_start = start;
        let key_len_end = start + KEY_LEN_SIZE;
        let key_len = u16::from_le_bytes([buf[key_len_start], buf[key_len_start + 1]]);

        let key_start = key_len_end;
        let key_end = key_start + key_len as usize;
        let key = Bytes::copy_from_slice(&buf[key_start..key_end]);

        let ts_start = key_end;
        let ts_end = ts_start + TS_SIZE;
        let ts = u64::from_le_bytes([
            buf[ts_start],
            buf[ts_start + 1],
            buf[ts_start + 2],
            buf[ts_start + 3],
            buf[ts_start + 4],
            buf[ts_start + 5],
            buf[ts_start + 6],
            buf[ts_start + 7],
        ]);

        let value_len_start = ts_end;
        let value_len_end = value_len_start + VALUE_LEN_SIZE;
        let value_len = u16::from_le_bytes([buf[value_len_start], buf[value_len_start + 1]]);

        let value_start = value_len_end;
        let value_end = value_start + value_len as usize;
        let value = Bytes::copy_from_slice(&buf[value_start..value_end]);

        (KeyBytes::from_bytes_with_ts(key, ts), value)
    }

    fn parse_batch(buf: &[u8]) -> Vec<(KeyBytes, Bytes)> {
        let mut records = Vec::<(KeyBytes, Bytes)>::new();
        let mut start = 0;
        while start < buf.len() {
            let (key, value) = Self::read_next_record(buf, start);
            start += KEY_LEN_SIZE + key.key_len() + TS_SIZE + VALUE_LEN_SIZE + value.len();
            records.push((key, value));
        }
        records
    }

    fn read_next_raw_batch(reader: &mut BufReader<File>) -> Result<Option<Vec<u8>>> {
        let mut body_len_buf = [0u8; BODY_LEN_SIZE];
        reader.read_exact(&mut body_len_buf)?;
        let body_len = u32::from_le_bytes(body_len_buf) as usize;

        let mut body_buf = vec![0u8; body_len];
        reader.read_exact(&mut body_buf)?;
        let actual_checksum = crc32fast::hash(&body_buf);

        let mut checksum_buf = [0u8; CHECKSUM_SIZE];
        reader.read_exact(&mut checksum_buf)?;
        let checksum = u32::from_le_bytes(checksum_buf);

        if actual_checksum != checksum {
            return Err(anyhow!(
                "Checksum of WAL mismatched, expected: {}, actual: {}",
                checksum,
                actual_checksum
            ));
        }

        Ok(Some(body_buf))
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let file = File::open(&path)?;
        let file_size = file.metadata()?.size() as usize;
        let mut reader = BufReader::new(file);

        let mut start = 0;
        let mut max_ts = 0u64;
        while start < file_size {
            let raw_batch = Self::read_next_raw_batch(&mut reader)?;
            // Run into an incomplete WAL.
            if raw_batch.is_none() {
                break;
            }
            let raw_batch = raw_batch.unwrap();
            let records = Self::parse_batch(&raw_batch);
            for (key, value) in records {
                max_ts = max_ts.max(key.ts());
                skiplist.insert(key, value);
            }
            start += BODY_LEN_SIZE + raw_batch.len() + CHECKSUM_SIZE;
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
            max_ts: AtomicU64::new(max_ts),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    fn write_records_size(data: &[(KeySlice, &[u8])], writer: &mut File) -> Result<()> {
        let mut size = 0;
        for (key, value) in data {
            size += KEY_LEN_SIZE;
            size += key.key_len();
            size += TS_SIZE;
            size += VALUE_LEN_SIZE;
            size += value.len();
        }
        writer.write_all(&u32::to_le_bytes(size as u32))?;
        Ok(())
    }

    pub fn write_record(
        key: &KeySlice,
        value: &[u8],
        writer: &mut File,
        hasher: &mut Hasher,
    ) -> Result<()> {
        let key_len_bytes = u16::to_le_bytes(key.key_ref().len() as u16);
        writer.write_all(&key_len_bytes)?;
        hasher.update(&key_len_bytes);

        writer.write_all(key.key_ref())?;
        hasher.update(key.key_ref());

        let ts_bytes = u64::to_le_bytes(key.ts());
        writer.write_all(&ts_bytes)?;
        hasher.update(&ts_bytes);

        let value_len_bytes = u16::to_le_bytes(value.len() as u16);
        writer.write_all(&value_len_bytes)?;
        hasher.update(&value_len_bytes);

        writer.write_all(value)?;
        hasher.update(value);

        Ok(())
    }

    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        let mut guard = self.file.lock();
        let file = (*guard).get_mut();
        Self::write_records_size(data, file)?;

        let mut hasher = crc32fast::Hasher::new();
        let mut max_ts = 0u64;
        for (key, value) in data {
            Self::write_record(key, value, file, &mut hasher)?;
            max_ts = max_ts.max(key.ts());
        }
        self.max_ts
            .fetch_max(max_ts, std::sync::atomic::Ordering::Relaxed);

        let checksum = u32::to_le_bytes(hasher.finalize());
        file.write_all(&checksum)?;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut writer = self.file.lock();
        writer.flush()?;
        writer.get_mut().sync_all()?;
        Ok(())
    }
}
