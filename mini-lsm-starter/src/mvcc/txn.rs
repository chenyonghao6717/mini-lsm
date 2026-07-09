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

use std::{
    collections::HashSet,
    ops::Bound,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

use anyhow::{Result, anyhow};
use bytes::Bytes;
use crossbeam_skiplist::SkipMap;
use ouroboros::self_referencing;
use parking_lot::Mutex;

use crate::{
    iterators::{StorageIterator, two_merge_iterator::TwoMergeIterator},
    lsm_iterator::{FusedIterator, LsmIterator},
    lsm_storage::{LsmStorageInner, WriteBatchRecord},
};

pub struct Transaction {
    pub(crate) read_ts: u64,
    pub(crate) inner: Arc<LsmStorageInner>,
    pub(crate) local_storage: Arc<SkipMap<Bytes, Bytes>>,
    pub(crate) committed: Arc<AtomicBool>,
    /// Write set and read set
    pub(crate) key_hashes: Option<Mutex<(HashSet<u32>, HashSet<u32>)>>,
}

impl Transaction {
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.check_commit_status()?;
        if let Some(entry) = self.local_storage.get(key) {
            let value = entry.value();
            return Ok(if value.is_empty() {
                None
            } else {
                Some(value.clone())
            });
        }
        self.inner.get_with_ts(self.read_ts, key)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.check_commit_status()?;
        self.inner.scan_with_txn(Arc::clone(self), lower, upper)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.check_commit_status()?;
        self.local_storage
            .insert(Bytes::copy_from_slice(key), Bytes::copy_from_slice(value));
        Ok(())
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.check_commit_status()?;
        self.put(key, &[])
    }

    pub fn commit(&self) -> Result<()> {
        if self.committed.load(Ordering::Relaxed) {
            return Ok(());
        }

        let mut records = Vec::<WriteBatchRecord<Bytes>>::new();
        let local_iter = self.local_storage.iter();
        for entry in local_iter {
            let key = entry.key();
            let value = entry.value();
            records.push(if value.is_empty() {
                WriteBatchRecord::Del(Bytes::clone(key))
            } else {
                WriteBatchRecord::Put(Bytes::clone(key), Bytes::clone(value))
            })
        }
        self.inner.write_batch(&records)?;
        self.committed.store(true, Ordering::Relaxed);
        Ok(())
    }

    pub fn check_commit_status(&self) -> Result<()> {
        if self.committed.load(Ordering::Relaxed) {
            Err(anyhow!("Try to process a committed transaction!"))
        } else {
            Ok(())
        }
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        self.inner.mvcc().drop_read_ts(self.read_ts);
    }
}

type SkipMapRangeIter<'a> =
    crossbeam_skiplist::map::Range<'a, Bytes, (Bound<Bytes>, Bound<Bytes>), Bytes, Bytes>;

#[self_referencing]
pub struct TxnLocalIterator {
    /// Stores a reference to the skipmap.
    map: Arc<SkipMap<Bytes, Bytes>>,
    /// Stores a skipmap iterator that refers to the lifetime of `TxnLocalIterator` itself.
    #[borrows(map)]
    #[not_covariant]
    iter: SkipMapRangeIter<'this>,
    /// Stores the current key-value pair.
    item: (Bytes, Bytes),
}

impl TxnLocalIterator {
    pub fn create(
        map: Arc<SkipMap<Bytes, Bytes>>,
        lower: Bound<Bytes>,
        upper: Bound<Bytes>,
    ) -> Result<Self> {
        let mut self_ = TxnLocalIteratorBuilder {
            map,
            iter_builder: |map_ref| map_ref.range((lower, upper)),
            item: (Bytes::new(), Bytes::new()),
        }
        .build();
        self_.next()?;
        Ok(self_)
    }
}

impl StorageIterator for TxnLocalIterator {
    type KeyType<'a> = &'a [u8];

    fn key(&self) -> &[u8] {
        self.borrow_item().0.as_ref()
    }

    fn value(&self) -> &[u8] {
        self.borrow_item().1.as_ref()
    }

    fn is_valid(&self) -> bool {
        !self.key().is_empty()
    }

    fn next(&mut self) -> Result<()> {
        let next_entry = self
            .with_iter_mut(|iter| {
                iter.next().map(|e| {
                    (
                        Bytes::copy_from_slice(e.key()),
                        Bytes::copy_from_slice(e.value()),
                    )
                })
            })
            .unwrap_or((Bytes::new(), Bytes::new()));
        self.with_item_mut(|item| *item = next_entry);
        Ok(())
    }
}

pub struct TxnIterator {
    txn: Arc<Transaction>,
    iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
}

impl TxnIterator {
    pub fn create(
        txn: Arc<Transaction>,
        iter: TwoMergeIterator<TxnLocalIterator, FusedIterator<LsmIterator>>,
    ) -> Result<Self> {
        Ok(Self { txn, iter })
    }
}

impl StorageIterator for TxnIterator {
    type KeyType<'a>
        = &'a [u8]
    where
        Self: 'a;

    fn value(&self) -> &[u8] {
        self.iter.value()
    }

    fn key(&self) -> Self::KeyType<'_> {
        self.iter.key()
    }

    fn is_valid(&self) -> bool {
        self.iter.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        self.iter.next()?;
        while self.iter.is_valid() && self.iter.value().is_empty() {
            self.iter.next()?;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
