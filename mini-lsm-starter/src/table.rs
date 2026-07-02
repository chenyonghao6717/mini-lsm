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
use crate::block::KEY_LEN_SIZE;
use crate::key::{KeyBytes, KeySlice};
use crate::lsm_storage::BlockCache;

use self::bloom::Bloom;

const META_SECTION_OFFSET_SIZE: usize = 4;
const BLOOM_OFFSET_SIZE: usize = 4;
const NUM_OF_BLOCKS_SIZE: usize = 4;
const BLOCK_OFFSET_SIZE: usize = 4;
const META_OFFSET_SIZE: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlockMeta {
    /// Offset of this data block.
    pub offset: usize,
    /// The first key of the data block.
    pub first_key: KeyBytes,
    /// The last key of the data block.
    pub last_key: KeyBytes,
}

/// Block meta section layout:
/// |       block meta section    |    meta offsets section   |   num of blocks   |
/// | block meta 1 | block meta 2 | offset1 u32 | offset2 u32 |        u32        |
/// Block meta layout:
/// | offset | first_key | last_key | first_key_len |
/// |   u32  |           |          |     u16       |
impl BlockMeta {
    /// Encode block meta to a buffer.
    /// You may add extra fields to the buffer,
    /// in order to help keep track of `first_key` when decoding from the same buffer in the future.
    pub fn encode_block_meta(block_meta: &[BlockMeta], buf: &mut Vec<u8>) {
        let mut meta_offsets = Vec::<u32>::new();
        // Block meta section
        for meta in block_meta {
            let meta_offset = buf.len() as u32;
            meta_offsets.push(meta_offset);

            buf.put_u32_le(meta.offset as u32);
            buf.extend_from_slice(meta.first_key.raw_ref());
            buf.extend_from_slice(meta.last_key.raw_ref());
            buf.extend((meta.first_key.len() as u16).to_le_bytes());
        }
        let num_of_meta = meta_offsets.len() as u32;
        // Meta offsets section
        for offset in meta_offsets {
            buf.put_u32_le(offset);
        }
        // Num of meta
        buf.put_u32_le(num_of_meta);
    }

    fn extract_meta(meta_offsets: &[u32], meta_index: usize, meta_section: &[u8]) -> BlockMeta {
        let meta_start = meta_offsets[meta_index] as usize;
        let meta_end = {
            if meta_index == meta_offsets.len() - 1 {
                meta_section.len()
            } else {
                meta_offsets[meta_index + 1] as usize
            }
        };

        let raw_meta = &meta_section[meta_start..meta_end];
        let offset =
            u32::from_le_bytes([raw_meta[0], raw_meta[1], raw_meta[2], raw_meta[3]]) as usize;
        let first_key_len =
            u16::from_le_bytes([raw_meta[raw_meta.len() - 2], raw_meta[raw_meta.len() - 1]])
                as usize;

        let first_key_start = META_OFFSET_SIZE;
        let last_key_start = first_key_start + first_key_len;
        let last_key_end = raw_meta.len() - KEY_LEN_SIZE;

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

        let num_of_blocks = u32::from_le_bytes([
            raw[raw.len() - 4],
            raw[raw.len() - 3],
            raw[raw.len() - 2],
            raw[raw.len() - 1],
        ]) as usize;

        let meta_offsets_len = num_of_blocks * META_OFFSET_SIZE;
        let first_offset_index = raw.len() - NUM_OF_BLOCKS_SIZE - meta_offsets_len;

        let meta_offsets = raw[first_offset_index..raw.len() - NUM_OF_BLOCKS_SIZE]
            .chunks_exact(META_OFFSET_SIZE)
            .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect::<Vec<u32>>();

        let meta_section = &raw[0..first_offset_index];
        let mut metas = Vec::<BlockMeta>::new();
        for meta_index in 0..num_of_blocks {
            metas.push(Self::extract_meta(&meta_offsets, meta_index, meta_section))
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
/// -------------------------------------------------------------------------------------------
/// |         Block Section         |          Meta Section         |          Extra          |
/// -------------------------------------------------------------------------------------------
/// | data block | ... | data block |            metadata           | meta block offset (u32) |
/// -------------------------------------------------------------------------------------------
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

/// All ends are exclusive.
struct SectionRange {
    data_start: u64,
    data_end: u64,
    meta_start: u64,
    meta_end: u64,
    meta_offset_start: u64,
    meta_offset_end: u64,
    bloom_start: u64,
    bloom_end: u64,
    bloom_offset_start: u64,
    bloom_offset_end: u64,
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

    fn to_u32(bytes: &[u8]) -> u32 {
        u32::from_le_bytes(bytes[..4].try_into().unwrap())
    }

    /// See also SsTableBuilder::build
    /// -----------------------------------------------------------------------------------------------------
    /// |         Block Section         |                            Meta Section                           |
    /// -----------------------------------------------------------------------------------------------------
    /// | data block | ... | data block | metadata | meta block offset | bloom filter | bloom filter offset |
    /// |                               |  varlen  |         u32       |    varlen    |        u32          |
    /// -----------------------------------------------------------------------------------------------------
    fn get_section_range(file: &FileObject) -> Result<Option<SectionRange>> {
        if file.0.is_none() {
            return Ok(None);
        }

        let file_size = file.size();

        let bloom_offset_start = file_size - BLOOM_OFFSET_SIZE as u64;
        let bloom_offset_end = file_size;

        let bloom_offset_bytes = file.read(bloom_offset_start, BLOOM_OFFSET_SIZE as u64)?;
        let bloom_offset = Self::to_u32(&bloom_offset_bytes);

        let bloom_start = bloom_offset as u64;
        let bloom_end = bloom_offset_start;

        let meta_offset_start = bloom_start - META_SECTION_OFFSET_SIZE as u64;
        let meta_offset_end = bloom_start;

        let meta_offset_bytes = file.read(meta_offset_start, META_SECTION_OFFSET_SIZE as u64)?;
        let meta_offset = Self::to_u32(&meta_offset_bytes);

        let meta_start = meta_offset as u64;
        let meta_end = meta_offset_start;

        let data_start: u64 = 0;
        let data_end = meta_start;

        Ok(Some(SectionRange {
            data_start,
            data_end,
            meta_start,
            meta_end,
            meta_offset_start,
            meta_offset_end,
            bloom_start,
            bloom_end,
            bloom_offset_start,
            bloom_offset_end,
        }))
    }

    /// Open SSTable from a file.
    pub fn open(id: usize, block_cache: Option<Arc<BlockCache>>, file: FileObject) -> Result<Self> {
        let section_range = Self::get_section_range(&file)?;

        if section_range.is_none() {
            return Self::new(id);
        }

        let section_range = section_range.unwrap();

        let block_metas = {
            let buf_len = section_range.meta_end - section_range.meta_start;
            let raw_meta = file.read(section_range.meta_start, buf_len)?;
            BlockMeta::decode_block_meta(Cursor::new(raw_meta))
        };

        let bloom = {
            let buf_len = section_range.bloom_end - section_range.bloom_start;
            let raw_bloom = file.read(section_range.bloom_start, buf_len)?;
            Bloom::decode(&raw_bloom)
        }?;

        let first_key = block_metas
            .first()
            .map_or(KeyBytes::from_bytes(Bytes::new()), |x| x.first_key.clone());
        let last_key = block_metas
            .last()
            .map_or(KeyBytes::from_bytes(Bytes::new()), |x| x.last_key.clone());

        Ok(Self {
            file,
            block_meta: block_metas,
            block_meta_offset: section_range.meta_start as usize,
            id,
            block_cache,
            first_key,
            last_key,
            bloom: Some(bloom),
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

    pub fn may_contain(&self, key: &[u8]) -> bool {
        if let Some(bloom) = &self.bloom {
            bloom.may_contain(farmhash::fingerprint32(key))
        } else {
            true
        }
    }
}
