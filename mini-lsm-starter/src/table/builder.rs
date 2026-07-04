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

use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use std::{collections::HashSet, fs::OpenOptions};

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, FileObject, SsTable};
use crate::key::TS_DEFAULT;
use crate::{
    block::BlockBuilder,
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
    table::bloom::Bloom,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
    key_hash_values: HashSet<u32>,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            // Pre-allocate 256 MB to avoid expensive copies.
            data: Vec::with_capacity(256 * 1024 * 1024),
            meta: vec![BlockMeta {
                offset: 0,
                first_key: KeyBytes::from_bytes_with_ts(Bytes::new(), TS_DEFAULT),
                last_key: KeyBytes::from_bytes_with_ts(Bytes::new(), TS_DEFAULT),
            }],
            block_size,
            key_hash_values: HashSet::new(),
        }
    }

    /// Fill first_key and last_key of the current block meta.
    fn fill_block_meta_keys(&mut self) {
        let block_count = self.meta.len();
        let current_block_meta = &mut self.meta[block_count - 1];
        current_block_meta.first_key = self.builder.get_first_key();
        current_block_meta.last_key = self.builder.get_last_key();
    }

    /// Encode the current block into Vec<u8> and add it into self.data. Then create a new BlockBuilder.
    fn freeze_block(&mut self) {
        self.fill_block_meta_keys();

        // Freeze the current block and create a new block.
        let new_builder = BlockBuilder::new(self.block_size);
        let current_builder = std::mem::replace(&mut self.builder, new_builder);
        let current_block_data = current_builder.build().encode();

        self.data.extend_from_slice(&current_block_data);

        // Fill checksum
        let checksum = crc32fast::hash(&current_block_data);
        self.data.extend_from_slice(&u32::to_le_bytes(checksum));

        // Create a new meta for the newly created block.
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes_with_ts(Bytes::new(), TS_DEFAULT),
            last_key: KeyBytes::from_bytes_with_ts(Bytes::new(), TS_DEFAULT),
        });
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        self.key_hash_values
            .insert(farmhash::fingerprint32(key.key_ref()));

        if self.first_key.is_empty() {
            self.first_key = key.key_ref().to_vec();
        }

        while !self.builder.add(key, value) {
            self.freeze_block();
        }
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        self.builder.get_size() + self.data.len()
    }

    fn fill_keys(&mut self) {
        self.first_key = self.meta[0]
            .first_key
            .clone()
            .as_key_slice()
            .key_ref()
            .to_vec();
        self.last_key = self.meta[self.meta.len() - 1]
            .last_key
            .clone()
            .as_key_slice()
            .key_ref()
            .to_vec();
    }

    fn create_bloom(&self) -> Bloom {
        let bits_per_key = 10;
        let key_hash_values: &[u32] = &self.key_hash_values.iter().cloned().collect::<Vec<u32>>();
        Bloom::build_from_key_hashes(key_hash_values, bits_per_key)
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // Freeze the current block if it has data. Otherwise drop the empty block.
        if !self.builder.is_empty() {
            self.freeze_block();
        }

        // Each time self.freeze_block() is called, a new builder and a new meta
        // is created, so there is always an empty meta, we need to remove it.
        if self.meta.len() > 1 {
            self.meta.pop();
        }

        // Fill first_key and last_key
        self.fill_keys();

        // Encode meta
        let mut meta_buf = Vec::<u8>::new();
        BlockMeta::encode_block_meta(&self.meta, &mut meta_buf);

        // Encode bloom(bloom checksum is handled in Bloom::encode)
        let mut bloom_buf = Vec::<u8>::new();
        let bloom = self.create_bloom();
        bloom.encode(&mut bloom_buf);

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)?;

        // Write blocks
        file.write_all(self.data.as_ref())?;

        let meta_section_offset = file.metadata()?.len();
        // Write num of blocks.
        file.write_all(&u32::to_le_bytes(self.meta.len() as u32))?;
        // Write meta
        file.write_all(meta_buf.as_ref())?;
        // Write checksum
        let checksum = crc32fast::hash(meta_buf.as_ref());
        file.write_all(&u32::to_le_bytes(checksum))?;
        // Write meta offset
        file.write_all(&u32::to_le_bytes(meta_section_offset as u32))?;

        // Write bloom section
        let bloom_offset = file.metadata()?.len();
        file.write_all(bloom_buf.as_ref())?;
        file.write_all(&u32::to_le_bytes(bloom_offset as u32))?;
        file.flush()?;

        let file_meta = file.metadata()?;
        let file_size = file_meta.len();

        Ok(SsTable {
            file: FileObject(Some(file), file_size),
            block_meta: self.meta,
            meta_section_offset: meta_section_offset as usize,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes_with_ts(
                Bytes::copy_from_slice(&self.first_key),
                TS_DEFAULT,
            ),
            last_key: KeyBytes::from_bytes_with_ts(
                Bytes::copy_from_slice(&self.last_key),
                TS_DEFAULT,
            ),
            bloom: Some(bloom),
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
