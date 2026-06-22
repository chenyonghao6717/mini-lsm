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

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    fn search_table_idx(sstables: &[Arc<SsTable>], key: KeySlice) -> usize {
        // An empty key means we start from the first table.
        if key.is_empty() || sstables.is_empty() {
            0
        } else if key.raw_ref() > sstables.last().as_ref().unwrap().last_key().raw_ref() {
            sstables.len() + 1
        } else {
            let mut l = 0;
            let mut r = sstables.len() - 1;
            while l < r {
                let mid = l + (r - l) / 2;
                let table = &sstables[mid];
                if key.raw_ref() <= table.last_key().raw_ref() {
                    r = mid;
                } else {
                    l = mid + 1;
                }
            }
            l
        }
    }

    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        Ok(Self {
            current: sstables
                .first()
                .map(|table| SsTableIterator::create_and_seek_to_first(Arc::clone(table)))
                .transpose()?,
            next_sst_idx: 1,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let table_idx = Self::search_table_idx(&sstables, key);
        let table = sstables.get(table_idx);
        if let Some(t) = table {
            let iter = SsTableIterator::create_and_seek_to_key(Arc::clone(t), key)?;
            Ok(Self {
                current: Some(iter),
                next_sst_idx: table_idx + 1,
                sstables,
            })
        } else {
            Ok(Self {
                current: None,
                next_sst_idx: table_idx + 1,
                sstables,
            })
        }
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice<'_> {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.as_ref().is_some_and(|iter| iter.is_valid())
    }

    fn next(&mut self) -> Result<()> {
        if let Some(iter) = &mut self.current {
            iter.next()?;
            if !iter.is_valid() {
                let next_iter = self
                    .sstables
                    .get(self.next_sst_idx)
                    .map(|table| SsTableIterator::create_and_seek_to_first(Arc::clone(table)))
                    .transpose()?;
                self.current = next_iter;
                self.next_sst_idx += 1;
            }
            Ok(())
        } else {
            Ok(())
        }
    }

    fn num_active_iterators(&self) -> usize {
        self.sstables.len()
    }
}
