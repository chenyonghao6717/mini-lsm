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

use std::ops::Bound;

use anyhow::{Result, anyhow};
use bytes::Bytes;

use crate::{
    iterators::{
        StorageIterator, concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>,
    MergeIterator<SstConcatIterator>,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    lower: Bound<Bytes>,
    upper: Bound<Bytes>,
    prev_key: Vec<u8>,
    read_ts: u64,
}

impl LsmIterator {
    pub(crate) fn new(
        iter: LsmIteratorInner,
        lower: Bound<Bytes>,
        upper: Bound<Bytes>,
        read_ts: u64,
    ) -> Result<Self> {
        let mut self_ = Self {
            inner: iter,
            lower,
            upper,
            prev_key: Vec::new(),
            read_ts,
        };
        self_.skip_until_valid()?;
        Ok(self_)
    }

    /// Seeks to the next valid key if the current key is invalid, otherwise does nothing.
    fn skip_until_valid(&mut self) -> Result<()> {
        if let Bound::Excluded(key) = &self.lower {
            while self.is_valid() && self.inner.key().key_ref() == key {
                self.inner.next()?;
            }
        }
        while self.is_valid() {
            if self.inner.key().ts() > self.read_ts {
                self.inner.next()?;
            } else if self.inner.key().key_ref() == self.prev_key.as_slice() {
                self.inner.next()?;
            } else if self.inner.value().is_empty() {
                // All keys older than the tombstone should be skipped.
                self.prev_key = self.inner.key().key_ref().to_vec();
                self.inner.next()?;
            } else {
                break;
            }
        }
        if self.is_valid() {
            self.prev_key = self.inner.key().key_ref().to_vec();
        }
        Ok(())
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        if !self.inner.is_valid() {
            return false;
        }
        match &self.upper {
            Bound::Unbounded => true,
            Bound::Included(last_key) => self.inner.key().key_ref() <= last_key.as_ref(),
            Bound::Excluded(last_key) => self.inner.key().key_ref() < last_key.as_ref(),
        }
    }

    fn key(&self) -> &[u8] {
        self.inner.key().key_ref()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        self.inner.next()?;
        self.skip_until_valid()
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        if self.has_errored {
            false
        } else {
            self.iter.is_valid()
        }
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        if self.has_errored {
            return Err(anyhow!("Try to call next() on an invalid iterator!"));
        }
        if !self.iter.is_valid() {
            return Ok(());
        }
        let result = self.iter.next();
        if result.is_err() {
            self.has_errored = true;
        }
        result
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
