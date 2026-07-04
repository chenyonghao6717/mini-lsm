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

use bytes::{BufMut, Bytes};

use crate::key::{KeyBytes, KeySlice, TS_DEFAULT, TS_SIZE};

use super::{Block, KEY_LEN_SIZE, NUM_OF_ELEMENTS_SIZE, OFFSET_SIZE, VALUE_LEN_SIZE};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size. Unit: byte
    block_size: usize,
    /// The first key in the block
    first_key: Vec<u8>,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: Vec::new(),
        }
    }

    fn get_entry_size(key: &[u8], value: &[u8]) -> usize {
        // Each k-v pair needs a u16 to store the key len, a u16 to store the value len.
        KEY_LEN_SIZE + VALUE_LEN_SIZE + key.len() + value.len()
    }

    fn exceed_block_size(&self, entry_size: usize) -> bool {
        let total_size_after_add = self.data.len()
            + self.offsets.len() * OFFSET_SIZE
            + NUM_OF_ELEMENTS_SIZE
            + entry_size
            // The offset of the entry to add
            + OFFSET_SIZE;

        total_size_after_add > self.block_size
    }

    /// For a non-first key, a u16 is used to indicate how many bytes of it are the same
    /// as the first key, a u16 to indicate the rest part.
    /// DO NOT encode the first key since we don't store the complete first key elsewhere.
    /// An encoded key looks like:
    /// | key_overlap_len (u16) | rest_key_len (u16) | key (rest_key_len) | timestamp (u32) |
    fn encode_key(key: KeySlice, first_key: &[u8]) -> Vec<u8> {
        let mut overlap_len: u16 = 0;
        for (byte1, byte2) in key.key_ref().iter().zip(first_key.iter()) {
            if byte1 == byte2 {
                overlap_len += 1;
            } else {
                break;
            }
        }

        let rest_len = key.key_len() as u16 - overlap_len;
        let mut encoded_key = Vec::<u8>::new();

        encoded_key.extend_from_slice(&overlap_len.to_le_bytes());
        encoded_key.extend_from_slice(&rest_len.to_le_bytes());
        encoded_key.extend_from_slice(&key.key_ref()[overlap_len as usize..]);
        encoded_key.extend_from_slice(&u64::to_le_bytes(key.ts()));

        encoded_key
    }

    /// See also Self::encode_key
    pub fn decode_key(encoded_key: &[u8], first_key: &[u8]) -> KeyBytes {
        let raw_ts = &encoded_key[encoded_key.len() - TS_SIZE..];
        let ts = u64::from_le_bytes([
            raw_ts[0], raw_ts[1], raw_ts[2], raw_ts[3], raw_ts[4], raw_ts[5], raw_ts[6], raw_ts[7],
        ]);

        let overlap_len = u16::from_le_bytes([encoded_key[0], encoded_key[1]]);
        let rest_len = u16::from_le_bytes([encoded_key[2], encoded_key[3]]);
        let rest_key = &encoded_key[2 * size_of::<u16>()..encoded_key.len() - TS_SIZE];

        let mut decoded_key = first_key[..overlap_len as usize].to_vec();
        decoded_key.extend_from_slice(rest_key);
        KeyBytes::from_bytes_with_ts(Bytes::from_owner(decoded_key), ts)
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    /// You may find the `bytes::BufMut` trait useful for manipulating binary data.
    /// A block always accepts data if it doesn't have any data already. Even if the
    /// input data is larger than the threshold.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // We don't encode the first key.
        let encoded_key = if self.first_key.is_empty() {
            self.first_key = key.key_ref().to_vec();
            let mut full_key = key.key_ref().to_vec();
            full_key.extend_from_slice(&u64::to_be_bytes(key.ts()));
            full_key
        } else {
            Self::encode_key(key, &self.first_key)
        };

        let entry_size = Self::get_entry_size(&encoded_key, value);
        if !self.offsets.is_empty() && self.exceed_block_size(entry_size) {
            return false;
        }

        let offset = self.data.len() as u16;
        self.offsets.push(offset);

        self.data.put_u16_le(encoded_key.len() as u16);
        self.data.put_slice(&encoded_key);
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
        self.data.len() + self.offsets.len() * OFFSET_SIZE
    }

    pub fn get_key(&self, index: usize) -> KeyBytes {
        if index >= self.offsets.len() {
            KeyBytes::from_bytes_with_ts(Bytes::new(), TS_DEFAULT)
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
            let key_start = entry_start + KEY_LEN_SIZE;
            let key_end = key_start + key_len;
            let encoded_key = self.data[key_start..key_end].to_vec();

            // The first key is not encoded.
            if index == 0 {
                KeyBytes::from_bytes(Bytes::copy_from_slice(&encoded_key))
            } else {
                Self::decode_key(&encoded_key, &self.first_key)
            }
        }
    }

    pub fn get_first_key(&self) -> KeyBytes {
        self.get_key(0)
    }

    pub fn get_last_key(&self) -> KeyBytes {
        if self.offsets.is_empty() {
            KeyBytes::from_bytes_with_ts(Bytes::new(), TS_DEFAULT)
        } else {
            self.get_key(self.offsets.len() - 1)
        }
    }
}
