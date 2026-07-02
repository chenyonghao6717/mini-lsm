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

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::{Block, KEY_LEN_SIZE, VALUE_LEN_SIZE};
use crate::block::builder::BlockBuilder;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    /// An entry: |2 bytes length of key | key bytes | 2 bytes of length of value | value bytes |
    /// Returns a tuple of 4 numbers for the start(inclusive) and the end index(exclusive) of the key and value
    /// Returns 4 0 if the data has no entries after entry_start_index.
    fn get_kv_range(data: &[u8], entry_start: usize) -> Option<(usize, usize, usize, usize)> {
        if entry_start >= data.len() {
            return None;
        }

        let key_len = u16::from_le_bytes([data[entry_start], data[entry_start + 1]]) as usize;
        let key_start = entry_start + KEY_LEN_SIZE;
        let key_end = key_start + key_len;

        let value_len = u16::from_le_bytes([data[key_end], data[key_end + 1]]) as usize;
        let value_start = key_end + VALUE_LEN_SIZE;
        let value_end = value_start + value_len;

        Some((key_start, key_end, value_start, value_end))
    }

    fn get_key(
        data: &[u8],
        key_start: usize,
        key_end: usize,
        index: usize,
        // If the key to be gotton is already the first key, this arg can be any value.
        first_key: &[u8],
    ) -> Vec<u8> {
        let encoded_key = &data[key_start..key_end];
        if index == 0 {
            // The first key is not encoded.
            encoded_key.to_vec()
        } else {
            BlockBuilder::decode_key(encoded_key, first_key)
        }
    }

    fn new(block: Arc<Block>) -> Self {
        let first_entry_start = 0;
        let first_entry_idx = 0;
        if let Some((key_start, key_end, value_start, value_end)) =
            Self::get_kv_range(&block.data, first_entry_start)
        {
            let first_key = Self::get_key(&block.data, key_start, key_end, first_entry_idx, &[]);
            Self {
                block,
                key: KeyVec::from_vec(first_key.clone()),
                value_range: (value_start, value_end),
                idx: first_entry_idx,
                first_key: KeyVec::from_vec(first_key),
            }
        } else {
            Self {
                block,
                key: KeyVec::new(),
                value_range: (0, 0),
                idx: first_entry_idx,
                first_key: KeyVec::new(),
            }
        }
    }

    /// Returns key_start, key_end, value_start, value_end, index of the first key >= the given key
    fn find_kv_range(
        block: &Block,
        key: KeySlice,
        first_key: &[u8],
    ) -> Option<(usize, usize, usize, usize, usize)> {
        let mut l = 0;
        let mut r = block.offsets.len() - 1;
        while l < r {
            let mid = (l + r) >> 1;
            let offset = block.offsets[mid];
            if let Some((key_start, key_end, value_start, value_end)) =
                Self::get_kv_range(&block.data, offset as usize)
            {
                let mid_key = Self::get_key(&block.data, key_start, key_end, mid, first_key);
                if key.raw_ref() <= mid_key.as_slice() {
                    r = mid;
                } else {
                    l = mid + 1;
                }
            } else {
                return None;
            }
        }
        Self::get_kv_range(&block.data, block.offsets[l] as usize).map(
            |(key_start, key_end, value_start, value_end)| {
                (key_start, key_end, value_start, value_end, l)
            },
        )
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let first_entry_start = 0;
        let first_entry_idx = 0;

        let mut self_ = Self::new(Arc::clone(&block));

        let kv_range = Self::get_kv_range(&block.data, first_entry_start);
        if let Some((key_start, key_end, value_start, value_end)) = kv_range {
            let key = Self::get_key(&block.data, key_start, key_end, first_entry_idx, &[]);
            self_.key = KeyVec::from_vec(key);
        }
        self_
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut self_ = Self::new(Arc::clone(&block));
        self_.seek_to_key(key);
        self_
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice<'_> {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        if self.key.is_empty() {
            &[]
        } else {
            let (start, end) = self.value_range;
            &self.block.data[start..end]
        }
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        let (key_start, key_end, value_start, value_end, _) = Self::find_kv_range(
            &self.block,
            self.first_key.as_key_slice(),
            self.first_key.raw_ref(),
        )
        .unwrap();
        self.key = self.first_key.clone();
        self.value_range = (value_start, value_end);
        self.idx = 0;
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        let next_kv_range = Self::get_kv_range(&self.block.data, self.value_range.1);
        if let Some((key_start, key_end, value_start, value_end)) = next_kv_range {
            let next_key = Self::get_key(
                &self.block.data,
                key_start,
                key_end,
                self.idx + 1,
                self.first_key.raw_ref(),
            );
            self.key = KeyVec::from_vec(next_key);
            self.value_range = (value_start, value_end);
            self.idx += 1;
        } else {
            self.key = KeyVec::new()
        }
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        if let Some((key_start, key_end, value_start, value_end, index)) =
            Self::find_kv_range(&self.block, key, self.first_key.raw_ref())
        {
            let key = Self::get_key(
                &self.block.data,
                key_start,
                key_end,
                index,
                self.first_key.raw_ref(),
            );
            self.key = KeyVec::from_vec(key);
            self.value_range = (value_start, value_end);
            self.idx = index;
        } else {
            self.key = KeyVec::new();
        }
    }
}
