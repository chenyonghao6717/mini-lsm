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

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use bytes::Bytes;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::BlockBuilder,
    key::{Key, KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

pub const META_BLOCK_OFFSET_BYTES: u32 = 4;

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: vec![BlockMeta {
                offset: 0,
                first_key: KeyBytes::from_bytes(Bytes::new()),
                last_key: KeyBytes::from_bytes(Bytes::new()),
            }],
            block_size,
        }
    }

    /// Encode the current block into Vec<u8> and add it into self.data. Then create a new BlockBuilder.
    fn freeze_block(&mut self) {
        // Fill first_key and last_key of the current block meta.
        let block_count = self.meta.len();
        let current_block_meta = &mut self.meta[block_count - 1];
        current_block_meta.first_key = Key::from_bytes(Bytes::from(self.builder.get_first_key()));
        current_block_meta.last_key = Key::from_bytes(Bytes::from(self.builder.get_last_key()));

        // Freeze the current block and create a new block.
        let new_builder = BlockBuilder::new(self.block_size);
        let current_builder = std::mem::replace(&mut self.builder, new_builder);
        let current_block_data = current_builder.build().encode();
        self.data.append(&mut current_block_data.to_vec());

        // Create a new meta for the newly created block.
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: KeyBytes::from_bytes(Bytes::new()),
            last_key: KeyBytes::from_bytes(Bytes::new()),
        });
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        if self.first_key.is_empty() {
            self.first_key = key.raw_ref().to_vec();
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
        // self.data.len() + self.builder.get_size()
        self.data.len()
    }

    fn fill_keys(&mut self) {
        self.first_key = self.meta[0]
            .first_key
            .clone()
            .as_key_slice()
            .raw_ref()
            .to_vec();
        self.last_key = self.meta[self.meta.len() - 1]
            .last_key
            .clone()
            .as_key_slice()
            .raw_ref()
            .to_vec();
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    /// | block section | meta section |          extra          |
    /// |               |              |  meta block offset u32  |
    pub fn build(
        #[allow(unused_mut)] mut self,
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

        // Encode meta.
        let mut meta_buf = Vec::<u8>::new();
        BlockMeta::encode_block_meta(&self.meta, &mut meta_buf);

        // The beginning index of offset section.
        let meta_section_offset = self.data.len();

        let mut file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(path)?;

        file.write_all(self.data.as_ref())?;
        file.write_all(meta_buf.as_ref())?;
        file.write_all(&u32::to_le_bytes(meta_section_offset as u32))?;
        file.flush()?;
        let file_meta = file.metadata()?;
        let file_size = file_meta.len();

        Ok(SsTable {
            file: FileObject(Some(file), file_size),
            block_meta: self.meta,
            block_meta_offset: meta_section_offset,
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.first_key)),
            last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(&self.last_key)),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
