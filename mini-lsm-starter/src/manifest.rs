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

use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;
use crate::table::CHECKSUM_SIZE;

const RECORD_LEN_SIZE: usize = 4;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

/// Manifest record layout
/// | len(u32) | JSON record | checksum(u32) |
#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.as_ref())?;

        Ok(Manifest {
            file: Arc::new(Mutex::new(file)),
        })
    }

    fn to_pure_records(data: &[u8]) -> Result<Vec<u8>> {
        let mut pure_records = Vec::<u8>::new();
        let mut i = 0_usize;
        while i < data.len() {
            let raw_len = &data[i..i + RECORD_LEN_SIZE];
            let record_len =
                u32::from_le_bytes([raw_len[0], raw_len[1], raw_len[2], raw_len[3]]) as usize;

            let pure_record_start = i + RECORD_LEN_SIZE;
            let pure_record_end = i + record_len - CHECKSUM_SIZE;
            let pure_record = &data[pure_record_start..pure_record_end];

            let raw_checksum = &data[i + record_len - CHECKSUM_SIZE..i + record_len];
            let checksum = u32::from_le_bytes([
                raw_checksum[0],
                raw_checksum[1],
                raw_checksum[2],
                raw_checksum[3],
            ]);

            let actual_checksum = crc32fast::hash(pure_record);
            if actual_checksum != checksum {
                return Err(anyhow!(
                    "Checksum of manifest recode mismatched, expected: {}, actual: {}",
                    checksum,
                    actual_checksum
                ));
            }
            pure_records.extend_from_slice(pure_record);

            i += record_len;
        }
        Ok(pure_records)
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(path.as_ref())?;

        let mut data = Vec::<u8>::new();
        file.read_to_end(&mut data)?;
        let pure_records = Self::to_pure_records(&data)?;

        let stream = serde_json::Deserializer::from_slice(&pure_records);
        let mut records = Vec::<ManifestRecord>::new();
        for record in stream.into_iter::<ManifestRecord>() {
            records.push(record?);
        }

        Ok((
            Manifest {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        // The LSM state should be locked before writing a manifest record.
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let json_bytes = serde_json::to_vec(&record)?;
        let checksum = crc32fast::hash(&json_bytes);
        let record_len = RECORD_LEN_SIZE + json_bytes.len() + CHECKSUM_SIZE;

        let mut file = self.file.lock();
        file.write_all(&u32::to_le_bytes(record_len as u32))?;
        file.write_all(&json_bytes)?;
        file.write_all(&u32::to_le_bytes(checksum))?;

        file.flush()?;
        file.sync_all()?;
        Ok(())
    }
}
