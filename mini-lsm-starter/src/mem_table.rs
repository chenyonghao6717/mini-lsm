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

use std::ops::Bound;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::Result;
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use crossbeam_skiplist::map::Entry;
use ouroboros::self_referencing;

use crate::iterators::StorageIterator;
use crate::key::{KeyBytes, KeySlice, TS_DEFAULT};
use crate::lsm_storage::LsmStorageInner;
use crate::table::SsTableBuilder;
use crate::wal::Wal;

/// A basic mem-table based on crossbeam-skiplist.
///
/// An initial implementation of memtable is part of week 1, day 1. It will be incrementally implemented in other
/// chapters of week 1 and week 2.
pub struct MemTable {
    // map: Arc<SkipMap<Bytes, Bytes>>,
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    wal: Option<Wal>,
    id: usize,
    approximate_size: Arc<AtomicUsize>,
}

/// Create a bound of `Bytes` from a bound of `&[u8]`.
pub(crate) fn map_bound(bound: Bound<&[u8]>) -> Bound<Bytes> {
    match bound {
        Bound::Included(x) => Bound::Included(Bytes::copy_from_slice(x)),
        Bound::Excluded(x) => Bound::Excluded(Bytes::copy_from_slice(x)),
        Bound::Unbounded => Bound::Unbounded,
    }
}

impl MemTable {
    /// Create a new mem-table.
    pub fn create(id: usize) -> Self {
        Self {
            id,
            map: Arc::new(SkipMap::new()),
            wal: None,
            approximate_size: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Create a new mem-table with WAL
    pub fn create_with_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let wal_path = LsmStorageInner::path_of_wal_static(path, id);
        Ok(Self {
            id,
            map: Arc::new(SkipMap::new()),
            wal: Some(Wal::create(wal_path)?),
            approximate_size: Arc::new(AtomicUsize::new(0)),
        })
    }

    /// Create a memtable from WAL
    pub fn recover_from_wal(id: usize, path: impl AsRef<Path>) -> Result<Self> {
        let mut memtable = Self::create(id);
        let wal_path = LsmStorageInner::path_of_wal_static(path, id);
        let wal = Wal::recover(&wal_path, &memtable.map)?;
        memtable.wal = Some(wal);
        Ok(memtable)
    }

    pub fn for_testing_put_slice(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.put(KeySlice::from_slice(key, TS_DEFAULT), value)
    }

    pub fn for_testing_get_slice(&self, key: &[u8]) -> Option<Bytes> {
        self.get(KeySlice::from_slice(key, TS_DEFAULT))
    }

    pub fn for_testing_scan_slice(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> MemTableIterator {
        // This function is only used in week 1 tests, so during the week 3 key-ts refactor, you do
        // not need to consider the bound exclude/include logic. Simply provide `DEFAULT_TS` as the
        // timestamp for the key-ts pair.
        let lower = lower.map(|key| KeySlice::from_slice(key, TS_DEFAULT));
        let upper = upper.map(|key| KeySlice::from_slice(key, TS_DEFAULT));
        self.scan(lower, upper)
    }

    pub fn get_max_ts(&self) -> u64 {
        self.wal.as_ref().map(|w| w.get_max_ts()).unwrap_or(0)
    }

    /// Get a value by key.
    /// In week3 this function is only for testing.
    pub fn get(&self, key: KeySlice) -> Option<Bytes> {
        let raw_key_bytes = Bytes::from_static(unsafe {
            std::mem::transmute::<&[u8], &'static [u8]>(key.key_ref())
        });
        let key_bytes = KeyBytes::from_bytes_with_ts(raw_key_bytes, key.ts());
        self.map.get(&key_bytes).map(|x| x.value().clone())
    }

    /// Put a key-value pair into the mem-table.
    ///
    /// In week 1, day 1, simply put the key-value pair into the skipmap.
    /// In week 2, day 6, also flush the data to WAL.
    /// In week 3, day 5, modify the function to use the batch API.
    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5; if you want to implement this earlier, use `&[u8]` as the key type.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        if let Some(wal) = &self.wal {
            wal.put_batch(data)?;
        }

        for (key, value) in data {
            let key = KeyBytes::from_bytes_with_ts(Bytes::copy_from_slice(key.key_ref()), key.ts());
            let value = Bytes::copy_from_slice(value);
            let entry_len = key.raw_len() + value.len();
            self.map.insert(key, value);
            self.approximate_size
                .fetch_add(entry_len, std::sync::atomic::Ordering::AcqRel);
        }

        Ok(())
    }

    pub fn sync_wal(&self) -> Result<()> {
        if let Some(ref wal) = self.wal {
            wal.sync()?;
        }
        Ok(())
    }

    /// Get an iterator over a range of keys.
    pub fn scan(&self, lower: Bound<KeySlice>, upper: Bound<KeySlice>) -> MemTableIterator {
        let mut iter = MemTableIterator::new(
            Arc::clone(&self.map),
            |map_ref| map_ref.range((convert_bound(&lower), convert_bound(&upper))),
            to_iter_entry(None),
        );
        // Call next() once to load the first entry into item field.
        let _ = iter.next();
        iter
    }

    /// Flush the mem-table to SSTable. Implement in week 1 day 6.
    pub fn flush(&self, builder: &mut SsTableBuilder) -> Result<()> {
        let iter = self.map.iter();
        for entry in iter {
            builder.add(
                KeySlice::from_slice(entry.key().key_ref(), entry.key().ts()),
                entry.value(),
            )
        }
        Ok(())
    }

    pub fn id(&self) -> usize {
        self.id
    }

    pub fn approximate_size(&self) -> usize {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Only use this function when closing the database
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }
}

fn convert_bound(bound: &Bound<KeySlice>) -> Bound<KeyBytes> {
    match bound {
        Bound::Included(key) => Bound::Included(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(key.key_ref()),
            key.ts(),
        )),
        Bound::Excluded(key) => Bound::Excluded(KeyBytes::from_bytes_with_ts(
            Bytes::copy_from_slice(key.key_ref()),
            key.ts(),
        )),
        Bound::Unbounded => Bound::Unbounded,
    }
}

type SkipMapRangeIter<'a> = crossbeam_skiplist::map::Range<
    'a,
    KeyBytes,
    (Bound<KeyBytes>, Bound<KeyBytes>),
    KeyBytes,
    Bytes,
>;

/// An iterator over a range of `SkipMap`. This is a self-referential structure and please refer to week 1, day 2
/// chapter for more information.
///
/// This is part of week 1, day 2.
#[self_referencing]
pub struct MemTableIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<KeyBytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `MemTableIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (KeyBytes, Bytes),
}

fn to_iter_entry(map_entry: Option<Entry<'_, KeyBytes, Bytes>>) -> (KeyBytes, Bytes) {
    match map_entry {
        Some(entry) => (entry.key().clone(), entry.value().clone()),
        None => (
            KeyBytes::from_bytes_with_ts(Bytes::new(), TS_DEFAULT),
            Bytes::new(),
        ),
    }
}

impl StorageIterator for MemTableIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn value(&self) -> &[u8] {
        let value = &self.borrow_item().1;
        value.as_ref()
    }

    fn key(&self) -> KeySlice<'_> {
        let key = &self.borrow_item().0;
        KeySlice::from_slice(key.key_ref(), key.ts())
    }

    /// Returns whether there is still a valid value.
    fn is_valid(&self) -> bool {
        !self.borrow_item().0.is_empty()
    }

    /// Forwards cursor one step and put the new entry to item field.
    fn next(&mut self) -> Result<()> {
        let next_entry = self.with_iter_mut(|iter| to_iter_entry(iter.next()));
        self.with_item_mut(|item| *item = next_entry);

        Ok(())
    }
}
