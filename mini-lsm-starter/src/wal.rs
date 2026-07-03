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

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;
use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use crate::key::KeySlice;

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path.as_ref())?;
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    fn read_next_entry(reader: &mut BufReader<File>) -> Result<Option<(Bytes, Bytes)>> {
        let mut key_len_buf = [0u8; 2];
        match reader.read_exact(&mut key_len_buf) {
            Ok(()) => {
                let key_len = u16::from_le_bytes(key_len_buf) as usize;

                let mut key_buf = vec![0u8; key_len];
                reader.read_exact(&mut key_buf)?;
                let key = Bytes::from(key_buf);

                let mut value_len_buf = [0u8; 2];
                reader.read_exact(&mut value_len_buf)?;
                let value_len = u16::from_le_bytes(value_len_buf) as usize;

                let mut value_buf = vec![0u8; value_len];
                reader.read_exact(&mut value_buf)?;
                let value = Bytes::from(value_buf);

                Ok(Some((key, value)))
            }
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let file = File::open(&path)?;
        let mut reader = BufReader::new(file);

        while let Some((key, value)) = Self::read_next_entry(&mut reader)? {
            skiplist.insert(key, value);
        }

        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        Ok(Wal {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let mut writer = self.file.lock();
        writer.write_all(&(key.len() as u16).to_le_bytes())?;
        writer.write_all(key)?;
        writer.write_all(&(value.len() as u16).to_le_bytes())?;
        writer.write_all(value)?;

        Ok(())
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, _data: &[(KeySlice, &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut writer = self.file.lock();
        writer.flush()?;
        writer.get_mut().sync_all()?;
        Ok(())
    }
}
