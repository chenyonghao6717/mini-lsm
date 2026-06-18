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

pub(crate) mod bloom;
mod builder;
mod iterator;

use std::fs::File;
use std::io::Cursor;
use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Result, anyhow};
pub use builder::SsTableBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::SsTableIterator;

use crate::block::Block;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;
use super::block::{NUM_OF_ELEMENTS_BYTES, OFFSET_BYTES};

pub const BLOCK_META_OFFSET_BYTES: u64 = size_of::<u32>() as u64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

/// Encoded block metas:
/// | meta section |  meta offset section  | num of meta |
/// |              |    num of meta * u16  |    u16      |
/// An encoded meta:
/// | offset | first_key | last_key | first_key_len |
/// |   u16  |           |          |     u16       |
impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut offsets = Vec::<u16>::new();
        for meta in block_meta {
            offsets.push(buf.len() as u16);

            buf.extend_from_slice(&(meta.offset as u16).to_le_bytes());
            buf.extend_from_slice(meta.first_key.raw_ref());
            buf.extend_from_slice(meta.last_key.raw_ref());
            buf.extend((meta.first_key.len() as u16).to_le_bytes());
        }
        let num_of_meta = offsets.len() as u16;
        for offset in offsets {
            buf.put_u16_le(offset);
        }
        buf.put_u16_le(num_of_meta);
    }

    fn extract_meta(offsets: &[u16], meta_index: usize, meta_section: &[u8]) -> BlockMeta {
        let meta_start = offsets[meta_index] as usize;
        let meta_end = {
            if meta_index == offsets.len() - 1 {
                meta_section.len()
            } else {
                offsets[meta_index + 1] as usize
            }
        };

        let raw_meta = &meta_section[meta_start..meta_end];
        let offset = u16::from_le_bytes([raw_meta[0], raw_meta[1]]) as usize;
        let key_len =
            u16::from_le_bytes([raw_meta[raw_meta.len() - 2], raw_meta[raw_meta.len() - 1]])
                as usize;

        let first_key_start = OFFSET_BYTES;
        let last_key_start = OFFSET_BYTES + key_len;
        let last_key_end = raw_meta.len() - OFFSET_BYTES;

        let first_key = KeyBytes::from_bytes(Bytes::copy_from_slice(
            &raw_meta[first_key_start..last_key_start],
        ));
        let last_key = KeyBytes::from_bytes(Bytes::copy_from_slice(
            &raw_meta[last_key_start..last_key_end],
        ));
        BlockMeta {
            first_key,
            last_key,
            offset,
        }
    }

    /// Decode block meta from a buffer.
    pub fn decode_block_meta(buf: impl Buf) -> Vec<BlockMeta> {
        let raw = buf.chunk();

        let num_of_meta = u16::from_le_bytes([raw[raw.len() - 2], raw[raw.len() - 1]]) as usize;
        let first_offset_index = raw.len() - NUM_OF_ELEMENTS_BYTES - num_of_meta * OFFSET_BYTES;
        let offsets = raw[first_offset_index..raw.len() - 2]
            .chunks_exact(2)
            .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
            .collect::<Vec<u16>>();

        let meta_section = &raw[0..first_offset_index];
        let mut metas = Vec::<BlockMeta>::new();
        for meta_index in 0..num_of_meta {
            metas.push(Self::extract_meta(&offsets, meta_index, meta_section))
        }
        metas
    }
}

/// A file object.
pub struct FileObject(Option<File>, u64);

impl FileObject {
    pub fn read(&self, offset: u64, len: u64) -> Result<Vec<u8>> {
        use std::os::unix::fs::FileExt;
        let mut data = vec![0; len as usize];
        self.0
            .as_ref()
            .unwrap()
            .read_exact_at(&mut data[..], offset)?;
        Ok(data)
    }

    pub fn size(&self) -> u64 {
        self.1
    }

    /// Create a new file object (day 2) and write the file to the disk (day 4).
    pub fn create(path: &Path, data: Vec<u8>) -> Result<Self> {
        std::fs::write(path, &data)?;
        File::open(path)?.sync_all()?;
        Ok(FileObject(
            Some(File::options().read(true).write(false).open(path)?),
            data.len() as u64,
        ))
    }

    pub fn open(path: &Path) -> Result<Self> {
        let file = File::options().read(true).write(false).open(path)?;
        let size = file.metadata()?.len();
        Ok(FileObject(Some(file), size))
    }
}

/// An SSTable.
pub struct SsTable {
    /// The actual storage unit of SsTable, the format is as above.
    pub(crate) file: FileObject,
    /// The meta blocks that hold info for data blocks.
    pub(crate) block_meta: Vec<BlockMeta>,
    /// The offset that indicates the start point of meta blocks in `file`.
    pub(crate) block_meta_offset: usize,
    id: usize,
    block_cache: Option<Arc<BlockCache>>,
    first_key: KeyBytes,
    last_key: KeyBytes,
    pub(crate) bloom: Option<Bloom>,
    /// The maximum timestamp stored in this SST, implemented in week 3.
    max_ts: u64,
}

impl SsTable {
    #[cfg(test)]
    pub(crate) fn open_for_test(file: FileObject) -> Result<Self> {
        Self::open(0, None, file)
    }

    pub fn new(id: usize) -> Result<Self> {
        Ok(Self {
            file: FileObject(None, 0),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key: KeyBytes::from_bytes(Bytes::new()),
            last_key: KeyBytes::from_bytes(Bytes::new()),
            bloom: None,
            max_ts: 0,
        })
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        if file.0.is_none() {
            return Self::new(id);
        }

        let file_size = file.size();
        let block_meta_offset_bytes =
            file.read(file_size - BLOCK_META_OFFSET_BYTES, BLOCK_META_OFFSET_BYTES)?;

        if block_meta_offset_bytes.len() < BLOCK_META_OFFSET_BYTES as usize {
            return Self::new(id);
        }
        let block_meta_offset = u32::from_le_bytes(
            block_meta_offset_bytes[..BLOCK_META_OFFSET_BYTES as usize]
                .try_into()
                .unwrap(),
        ) as u64;

        let block_metas = if block_meta_offset > 0 {
            let buf_len = file_size - block_meta_offset - BLOCK_META_OFFSET_BYTES;
            let raw_meta = file.read(block_meta_offset, buf_len)?;
            BlockMeta::decode_block_meta(Cursor::new(raw_meta))
        } else {
            Vec::<BlockMeta>::new()
        };

        let first_key = block_metas
            .first()
            .map_or(KeyBytes::from_bytes(Bytes::new()), |x| x.first_key.clone());
        let last_key = block_metas
            .last()
            .map_or(KeyBytes::from_bytes(Bytes::new()), |x| x.last_key.clone());

        Ok(Self {
            file,
            block_meta: block_metas,
            block_meta_offset: block_meta_offset as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        })
    }

    /// Create a mock SST with only first key + last key metadata
    pub fn create_meta_only(
        id: usize,
        file_size: u64,
        first_key: KeyBytes,
        last_key: KeyBytes,
    ) -> Self {
        Self {
            file: FileObject(None, file_size),
            block_meta: vec![],
            block_meta_offset: 0,
            id,
            block_cache: None,
            first_key,
            last_key,
            bloom: None,
            max_ts: 0,
        }
    }

    /// Read a block from the disk.
    pub fn read_block(&self, block_idx: usize) -> Result<Arc<Block>> {
        if block_idx >= self.block_meta.len() {
            Err(anyhow!(
                "Block index {} out of bounds (max: {})",
                block_idx,
                self.block_meta.len()
            ))
        } else {
            let block_start = self.block_meta[block_idx].offset as u64;
            let block_end = if block_idx < self.block_meta.len() - 1 {
                self.block_meta[block_idx + 1].offset
            } else {
                self.block_meta_offset
            } as u64;

            let raw_block = self.file.read(block_start, block_end - block_start)?;
            let block = Block::decode(&raw_block);

            Ok(Arc::new(block))
        }
    }

    /// Read a block from disk, with block cache. (Day 4)
    pub fn read_block_cached(&self, block_idx: usize) -> Result<Arc<Block>> {
        if let Some(cache) = &self.block_cache {
            let block_result =
                cache.try_get_with((self.id, block_idx), || self.read_block(block_idx));
            block_result.map_err(|err| anyhow!("{}", err))
        } else {
            self.read_block(block_idx)
        }
    }

    /// Find the block that may contain `key`.
    /// Note: You may want to make use of the `first_key` stored in `BlockMeta`.
    /// You may also assume the key-value pairs stored in each consecutive block are sorted.
    pub fn find_block_idx(&self, key: KeySlice) -> usize {
        unimplemented!()
    }

    /// Get number of data blocks.
    pub fn num_of_blocks(&self) -> usize {
        self.block_meta.len()
    }

    pub fn first_key(&self) -> &KeyBytes {
        &self.first_key
    }

    pub fn last_key(&self) -> &KeyBytes {
        &self.last_key
    }

    pub fn table_size(&self) -> u64 {
        self.file.1
    }

    pub fn sst_id(&self) -> usize {
        self.id
    }

    pub fn max_ts(&self) -> u64 {
        self.max_ts
    }

    pub fn has_overlap(&self, _lower: Bound<&[u8]>, _upper: Bound<&[u8]>) -> bool {
        match _lower {
            Bound::Included(lower_key) => {
                if lower_key > self.last_key.as_key_slice().raw_ref() {
                    return false;
                }
            }
            Bound::Excluded(lower_key) => {
                if lower_key >= self.last_key.as_key_slice().raw_ref() {
                    return false;
                }
            }
            _ => {}
        }

        match _upper {
            Bound::Included(upper_key) => {
                if upper_key < self.first_key.as_key_slice().raw_ref() {
                    return false;
                }
            }
            Bound::Excluded(upper_key) => {
                if upper_key <= self.first_key.as_key_slice().raw_ref() {
                    return false;
                }
            }
            _ => {}
        }

        true
    }

    pub fn has_key(&self, key: &[u8]) -> bool {
        self.first_key.as_key_slice().raw_ref() <= key
            && key <= self.last_key.as_key_slice().raw_ref()
    }
}
