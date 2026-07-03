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

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{BufMut, Bytes, BytesMut};
pub use iterator::BlockIterator;

// Sizes of fields within a block
pub const KEY_LEN_SIZE: usize = 2;
pub const VALUE_LEN_SIZE: usize = 2;
pub const OFFSET_SIZE: usize = 2;
pub const NUM_OF_ELEMENTS_SIZE: usize = 2;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
/// ----------------------------------------------------------------------------------------------------
/// |             Data Section             |              Offset Section             |      Extra      |
/// ----------------------------------------------------------------------------------------------------
/// | Entry #1 | Entry #2 | ... | Entry #N | Offset #1 | Offset #2 | ... | Offset #N | num_of_elements |
/// ----------------------------------------------------------------------------------------------------
/// -----------------------------------------------------------------------
/// |                           Entry #1                            | ... |
/// -----------------------------------------------------------------------
/// | key_len (2B) | key (keylen) | value_len (2B) | value (varlen) | ... |
/// -----------------------------------------------------------------------
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let num_of_elements = self.offsets.len() as u16;

        let mut buf = BytesMut::with_capacity(
            self.data.len() + self.offsets.len() * OFFSET_SIZE + NUM_OF_ELEMENTS_SIZE,
        );

        buf.put_slice(&self.data);
        for offset in &self.offsets {
            buf.put_u16_le(*offset);
        }
        buf.put_u16_le(num_of_elements);

        buf.freeze()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let data_len = data.len();
        let num_of_elements = u16::from_le_bytes([data[data_len - 2], data[data_len - 1]]) as usize;

        let offsets_len = num_of_elements * OFFSET_SIZE;

        let first_offset_index = data_len - offsets_len - NUM_OF_ELEMENTS_SIZE;
        let offsets_raw = &data[first_offset_index..data_len - NUM_OF_ELEMENTS_SIZE];
        let offsets: Vec<u16> = offsets_raw
            .chunks_exact(OFFSET_SIZE)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect();

        let entries_len = data_len - offsets_len - NUM_OF_ELEMENTS_SIZE;
        let entryies = data[..entries_len].to_vec();

        Self {
            offsets,
            data: entryies,
        }
    }
}
