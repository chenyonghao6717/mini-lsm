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

use bytes::BufMut;

use crate::key::{Key, KeySlice, KeyVec};

use super::{Block, KEY_LEN_BYTES, NUM_OF_ELEMENTS_BYTES, OFFSET_BYTES, VAL_LEN_BYTES};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size. Unit: byte
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: Key::new(),
        }
    }

    fn get_entry_size(key: KeySlice, value: &[u8]) -> usize {
        // Each k-v pair needs a u16 to store the key len, a u16 to store the value len.
        KEY_LEN_BYTES + VAL_LEN_BYTES + key.len() + value.len()
    }

    fn exceed_block_size(&self, entry_size: usize) -> bool {
        let total_size_after_add = self.offsets.len() * OFFSET_BYTES
            + self.data.len()
            + NUM_OF_ELEMENTS_BYTES
            + entry_size
            + OFFSET_BYTES;

        total_size_after_add > self.block_size
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    /// A block always accepts data if it doesn't have any data already. Even if the
    /// input data is larger than the threshold.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let entry_size = Self::get_entry_size(key, value);
        if !self.offsets.is_empty() && self.exceed_block_size(entry_size) {
            return false;
        }

        let offset = self.data.len() as u16;
        self.offsets.push(offset);

        self.data.put_u16_le(key.len() as u16);
        self.data.put_slice(key.raw_ref());
        self.data.put_u16_le(value.len() as u16);
        self.data.put_slice(value);

        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    pub fn get_size(&self) -> usize {
        self.data.len() + self.offsets.len() * OFFSET_BYTES
    }

    pub fn get_key(&self, index: usize) -> Vec<u8> {
        if index >= self.offsets.len() {
            Vec::new()
        } else {
            let entry_start = self.offsets[index] as usize;
            let entry_end = {
                if index + 1 < self.offsets.len() {
                    self.offsets[index + 1] as usize
                } else {
                    self.data.len()
                }
            };
            let key_len =
                u16::from_le_bytes([self.data[entry_start], self.data[entry_start + 1]]) as usize;
            let key_start = entry_start + KEY_LEN_BYTES;
            let key_end = key_start + key_len;
            let mut key = Vec::new();
            key.extend_from_slice(&self.data[key_start..key_end]);
            key
        }
    }

    pub fn get_first_key(&self) -> Vec<u8> {
        self.get_key(0)
    }

    pub fn get_last_key(&self) -> Vec<u8> {
        if self.offsets.is_empty() {
            Vec::new()
        } else {
            self.get_key(self.offsets.len() - 1)
        }
    }
}
