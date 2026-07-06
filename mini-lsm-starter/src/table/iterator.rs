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

use super::SsTable;
use crate::{block::BlockIterator, iterators::StorageIterator, key::KeySlice};

/// An iterator over the contents of an SSTable.
pub struct SsTableIterator {
    table: Arc<SsTable>,
    blk_iter: BlockIterator,
    blk_idx: usize,
}

impl SsTableIterator {
    /// Create a new iterator and seek to the first key-value pair in the first data block.
    pub fn create_and_seek_to_first(table: Arc<SsTable>) -> Result<Self> {
        let first_block = table.read_block_cached(0)?;
        let iter = Self {
            table,
            blk_iter: BlockIterator::create_and_seek_to_first(first_block),
            blk_idx: 0,
        };
        Ok(iter)
    }

    /// Seek to the first key-value pair in the first data block.
    pub fn seek_to_first(&mut self) -> Result<()> {
        let first_block = self.table.read_block_cached(0)?;
        self.blk_iter = BlockIterator::create_and_seek_to_first(first_block);
        self.blk_idx = 0;
        Ok(())
    }

    /// Create a new iterator and seek to the first key-value pair which >= `key`.
    pub fn create_and_seek_to_key(table: Arc<SsTable>, key: KeySlice) -> Result<Self> {
        let first_block = table.read_block_cached(0)?;
        let mut iter = Self {
            table,
            blk_iter: BlockIterator::create_and_seek_to_first(first_block),
            blk_idx: 0,
        };
        iter.seek_to_key(key)?;
        Ok(iter)
    }

    /// Seek to the first key-value pair which >= `key`.
    /// Note: You probably want to review the handout for detailed explanation when implementing
    /// this function.
    pub fn seek_to_key(&mut self, key: KeySlice) -> Result<()> {
        // Find the block with index i that block[i].first_key <= key < block[i + 1].first_key
        let mut l = 0;
        let mut r = self.table.block_meta.len() - 1;

        while l < r {
            let mid = (l + r) >> 1;

            let meta = &self.table.block_meta[mid];
            let start_key = &meta.first_key;
            let end_key = &meta.last_key;

            if key < start_key.as_key_slice() {
                r = mid;
            } else if end_key.as_key_slice() < key {
                l = mid + 1;
            } else {
                l = mid;
                r = mid;
            }
        }

        let block = self.table.read_block_cached(l)?;
        self.blk_iter = BlockIterator::create_and_seek_to_key(block, key);
        self.blk_idx = l;

        Ok(())
    }
}

impl StorageIterator for SsTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    /// Return the `key` that's held by the underlying block iterator.
    fn key(&self) -> KeySlice<'_> {
        self.blk_iter.key()
    }

    /// Return the `value` that's held by the underlying block iterator.
    fn value(&self) -> &[u8] {
        self.blk_iter.value()
    }

    /// Return whether the current block iterator is valid or not.
    fn is_valid(&self) -> bool {
        self.blk_iter.is_valid()
    }

    /// Move to the next `key` in the block.
    /// Note: You may want to check if the current block iterator is valid after the move.
    fn next(&mut self) -> Result<()> {
        self.blk_iter.next();
        // If all data of the current block is consumed
        if !self.blk_iter.is_valid() {
            self.blk_idx += 1;
            if self.blk_idx < self.table.block_meta.len() {
                let next_block = self.table.read_block_cached(self.blk_idx)?;
                let iter = BlockIterator::create_and_seek_to_first(next_block);
                self.blk_iter = iter;
            }
        }

        Ok(())
    }
}
