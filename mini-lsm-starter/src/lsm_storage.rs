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

use std::cmp::max;
use std::collections::{BTreeMap, HashMap};
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use anyhow::{Result, anyhow};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, CompactionTask, LeveledCompactionController,
    LeveledCompactionOptions, SimpleLeveledCompactionController, SimpleLeveledCompactionOptions,
    TieredCompactionController,
};
use crate::iterators::StorageIterator;
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::key::{KeySlice, TS_RANGE_BEGIN, TS_RANGE_END};
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::{MemTable, MemTableIterator};
use crate::mvcc::LsmMvccInner;
use crate::mvcc::txn::{Transaction, TxnIterator, TxnLocalIterator};
use crate::mvcc::watermark::Watermark;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};
use crate::wal::Wal;

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

const BLOCK_CACHE_SIZE: u64 = 1024;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    /// A newly frozon memtable is inserted at index 0.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.flush_notifier.send(()).map_err(|e| anyhow!("{}", e))?;

        {
            let mut flush_thread = self.flush_thread.lock();
            if let Some(handle) = flush_thread.take() {
                handle.join().map_err(|e| anyhow!("{:?}", e))?;
            }
        }

        // Cleanup
        if !self.inner.options.enable_wal {
            {
                let state_lock_observer = self.inner.state_lock.lock();
                self.inner.force_freeze_memtable(&state_lock_observer)?;
            }
            let memtable_num = self.inner.state.read().imm_memtables.len();
            for _ in 0..memtable_num {
                self.inner.force_flush_next_imm_memtable()?;
            }
        }

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<Arc<Transaction>> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(&self, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    pub(crate) fn mvcc(&self) -> &LsmMvccInner {
        self.mvcc.as_ref().unwrap()
    }

    fn load_sst(id: usize, block_cache: &Arc<BlockCache>, path: &Path) -> Result<Arc<SsTable>> {
        let sst_path = Self::path_of_sst_static(path, id);
        let sst = SsTable::open(
            id,
            Some(Arc::clone(block_cache)),
            FileObject::open(&sst_path)?,
        )?;
        Ok(Arc::new(sst))
    }

    fn load_compaction_manifest(
        compaction_controller: &CompactionController,
        mut snapshot: LsmStorageState,
        task: &CompactionTask,
        new_sst_ids: &[usize],
        block_cache: &Arc<BlockCache>,
        path: &Path,
        in_recovery: bool,
    ) -> Result<LsmStorageState> {
        for new_sst_id in new_sst_ids {
            let sst = Self::load_sst(*new_sst_id, block_cache, path)?;
            snapshot.sstables.insert(*new_sst_id, sst);
        }
        let (mut new_state, sst_ids_to_delete) = compaction_controller.apply_compaction_result(
            &snapshot,
            task,
            new_sst_ids,
            in_recovery,
        );
        for id in &sst_ids_to_delete {
            new_state.sstables.remove(id);
        }
        Ok(new_state)
    }

    fn load_memtable_manifest(
        memtable_id: usize,
        mut snapshot: LsmStorageState,
        path: &Path,
    ) -> Result<LsmStorageState> {
        // The memtable is flushed already.
        if snapshot.sstables.contains_key(&memtable_id) {
            Ok(snapshot)
        } else {
            let memtable = MemTable::recover_from_wal(memtable_id, path)?;
            snapshot.imm_memtables.insert(0, Arc::new(memtable));
            Ok(snapshot)
        }
    }

    fn load_manifest(
        compaction_controller: &CompactionController,
        manifest_records: &[ManifestRecord],
        options: &LsmStorageOptions,
        block_cache: Arc<BlockCache>,
        path: &Path,
        manifest_path: &Path,
    ) -> Result<LsmStorageState> {
        let mut state = LsmStorageState::create(options);
        for record in manifest_records {
            state = match record {
                ManifestRecord::Flush(sst_id) => {
                    let sst = Self::load_sst(*sst_id, &block_cache, path)?;
                    state.sstables.insert(*sst_id, sst);
                    state.l0_sstables.insert(0, *sst_id);
                    state
                }
                ManifestRecord::Compaction(task, new_sst_ids) => Self::load_compaction_manifest(
                    compaction_controller,
                    state,
                    task,
                    new_sst_ids,
                    &block_cache,
                    path,
                    true,
                )?,
                ManifestRecord::NewMemtable(memtable_id) => {
                    Self::load_memtable_manifest(*memtable_id, state, path)?
                }
            }
        }

        // Sort all levels
        for (_, sst_ids) in &mut state.levels {
            sst_ids.sort_by_key(|id| state.sstables[id].first_key());
        }

        Ok(state)
    }

    fn create_mvcc(init_ts: u64) -> Option<LsmMvccInner> {
        Some(LsmMvccInner {
            write_lock: Mutex::new(()),
            commit_lock: Mutex::new(()),
            ts: Arc::new(Mutex::new((init_ts + 1, Watermark::new()))),
            committed_txns: Arc::new(Mutex::new(BTreeMap::new())),
        })
    }

    fn create_compaction_controller(options: &LsmStorageOptions) -> CompactionController {
        match &options.compaction_options {
            CompactionOptions::Leveled(opts) => {
                CompactionController::Leveled(LeveledCompactionController::new(opts.clone()))
            }
            CompactionOptions::Tiered(opts) => {
                CompactionController::Tiered(TieredCompactionController::new(opts.clone()))
            }
            CompactionOptions::Simple(opts) => {
                CompactionController::Simple(SimpleLeveledCompactionController::new(opts.clone()))
            }
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        }
    }

    fn get_init_ts(
        path: &Path,
        state: &LsmStorageState,
        options: &LsmStorageOptions,
        manifest_records: &[ManifestRecord],
    ) -> u64 {
        let max_sst_ts = state
            .sstables
            .values()
            .map(|sst| sst.max_ts())
            .max()
            .unwrap_or(0);
        let max_memtable_id = manifest_records
            .iter()
            .filter_map(|record| match record {
                ManifestRecord::NewMemtable(id) => {
                    let wal_path = Self::path_of_wal_static(path, *id);
                    Wal::read_max_ts(&wal_path).ok()
                }
                _ => None,
            })
            .max()
            .unwrap_or(0);
        max(max_sst_ts, max_memtable_id)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        let in_recovery = path.try_exists()?;

        let compaction_controller = Self::create_compaction_controller(&options);
        let block_cache = Arc::new(BlockCache::new(BLOCK_CACHE_SIZE));

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let (manifest, manifest_records, state) = if in_recovery {
            let manifest_path = path.join("MANIFEST");
            let (manifest, manifest_records) = Manifest::recover(&manifest_path)?;
            let state = Self::load_manifest(
                &compaction_controller,
                &manifest_records,
                &options,
                Arc::clone(&block_cache),
                path,
                &manifest_path,
            )?;
            (manifest, manifest_records, state)
        } else {
            let manifest = Manifest::create(&path.join("MANIFEST"))?;
            (manifest, vec![], LsmStorageState::create(&options))
        };

        let next_sst_id = if in_recovery {
            state.sstables.keys().max().copied().unwrap_or(0) + 1
        } else {
            1
        };

        let init_ts = if in_recovery {
            Self::get_init_ts(path, &state, &options, &manifest_records)
        } else {
            0
        };
        let mvcc = Self::create_mvcc(init_ts);

        Ok(Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        })
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Find the value of a key in sstables denoted by sst_ids. We don't convert
    /// a value with empty data(tombstone) to Option::None here.
    fn get_in_level(
        &self,
        engine: &LsmStorageState,
        _key: &[u8],
        sst_ids: &[usize],
    ) -> Result<Option<Bytes>> {
        for id in sst_ids {
            if let Some(sst) = engine.sstables.get(id) {
                if !sst.may_contain(_key) {
                    continue;
                }
                let iter = SsTableIterator::create_and_seek_to_key(
                    sst.clone(),
                    KeySlice::from_slice(_key, TS_RANGE_BEGIN),
                )?;
                let k = iter.key();
                let v = iter.value();
                if iter.key().key_ref() != _key {
                    continue;
                }
                return Ok(Some(Bytes::copy_from_slice(iter.value())));
            } else {
                return Err(anyhow!("Sstable {} not found!", id));
            }
        }

        Ok(None)
    }

    fn get_from_sstables(&self, engine: &LsmStorageState, _key: &[u8]) -> Result<Option<Bytes>> {
        let mut value: Option<Bytes> = None;
        let value_of_l0 = self.get_in_level(engine, _key, &engine.l0_sstables)?;
        if value_of_l0.is_some() {
            value = value_of_l0;
        } else {
            for (level, sst_ids) in &engine.levels {
                let value_of_level = self.get_in_level(engine, _key, sst_ids)?;
                if value_of_level.is_some() {
                    value = value_of_level;
                    break;
                }
            }
        }

        Ok(value)
    }

    pub fn get(self: &Arc<Self>, key: &[u8]) -> Result<Option<Bytes>> {
        self.get_with_ts(self.mvcc().latest_commit_ts(), key)
    }

    pub fn get_with_ts(self: &Arc<Self>, read_ts: u64, key: &[u8]) -> Result<Option<Bytes>> {
        let lower = Bound::Included(key);
        let upper = Bound::Included(key);
        let iter = self.create_lsm_iter(lower, upper, read_ts)?;
        if iter.is_valid() && iter.key() == key && !iter.value().is_empty() {
            Ok(Some(Bytes::copy_from_slice(iter.value())))
        } else {
            Ok(None)
        }
    }

    pub fn put_with_ts(&self, key: &[u8], value: &[u8], ts: u64) -> Result<()> {
        // Only the read lock is required because only the internal state of the
        // engine is being modified, the engine itself stays the same.
        // The write lock is required only when the engine itself is replaced, that is,
        // when freezing the current memtable and creating a new one.
        // By using CoW, the writer trying to freeze memtable doesn't need to wait
        // readers to finish because it copies a new one. The write lock only blocks
        // later readers and writer from seeing a mid-state engine.
        let needs_freeze = {
            let engine = self.state.read();
            engine.memtable.put(KeySlice::from_slice(key, ts), value)?;
            engine.memtable.approximate_size() > self.options.target_sst_size
        };

        if needs_freeze {
            let state_lock = &self.state_lock.lock();
            let actual_needs_freeze = {
                let engine = self.state.read();
                engine.memtable.approximate_size() > self.options.target_sst_size
            };

            if actual_needs_freeze {
                self.force_freeze_memtable(state_lock)?;
            }
        }

        Ok(())
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        let _mvcc_guard = self.mvcc().write_lock.lock();
        let ts = self.mvcc().latest_commit_ts() + 1;
        for record in batch {
            match record {
                WriteBatchRecord::Put(key, value) => {
                    self.put_with_ts(key.as_ref(), value.as_ref(), ts)?
                }
                WriteBatchRecord::Del(key) => self.put_with_ts(key.as_ref(), &[], ts)?,
            }
        }
        self.mvcc().update_commit_ts(ts);
        Ok(())
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Put(key, value)])
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.write_batch(&[WriteBatchRecord::Del(key)])
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        let dir = File::open(&self.path)?;
        dir.sync_all()?;
        Ok(())
    }

    pub(crate) fn write_manifest_record(
        &self,
        record: ManifestRecord,
        _state_lock_observer: &MutexGuard<()>,
    ) -> Result<()> {
        if let Some(manifest) = &self.manifest {
            manifest.add_record(_state_lock_observer, record)?;
        }
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_engine = {
            let engine = {
                let guard = self.state.read();
                Arc::clone(&guard)
            };

            // Freeze memtable
            let mut new_engine = (*engine).clone();
            new_engine.memtable.sync_wal()?;
            new_engine
                .imm_memtables
                .insert(0, Arc::clone(&new_engine.memtable));

            // Create a new memtable
            let new_memtable_id = self.next_sst_id();
            if self.options.enable_wal {
                new_engine.memtable =
                    Arc::new(MemTable::create_with_wal(new_memtable_id, &self.path)?);
                self.write_manifest_record(
                    ManifestRecord::NewMemtable(new_memtable_id),
                    state_lock_observer,
                )?;
            } else {
                new_engine.memtable = Arc::new(MemTable::create(new_memtable_id));
            }

            new_engine
        };

        let mut engine = self.state.write();
        *engine = Arc::new(new_engine);

        Ok(())
    }

    fn get_earliest_imm_memtable(&self) -> Option<Arc<MemTable>> {
        let guard = self.state.read();
        guard.imm_memtables.last().cloned()
    }

    fn flush_earliest_memtable(&self, _state_lock_observer: &MutexGuard<()>) -> Result<()> {
        let mut new_engine = {
            let engine = self.state.read();
            (**engine).clone()
        };
        let memtable = new_engine.imm_memtables.last().unwrap();
        let sst_id = memtable.id();

        // Build SST (protected by self.state_lock)
        let mut table_builder = SsTableBuilder::new(self.options.block_size);
        memtable.flush(&mut table_builder)?;
        let sstable = Arc::new(table_builder.build(
            sst_id,
            Some(Arc::clone(&self.block_cache)),
            self.path_of_sst(sst_id),
        )?);

        // Sync folder after sst is created.
        self.sync_dir()?;

        // Write manifest.
        self.write_manifest_record(
            ManifestRecord::Flush(sstable.sst_id()),
            _state_lock_observer,
        )?;

        // Insert new sstable and remove memtable
        if self.compaction_controller.flush_to_l0() {
            new_engine.l0_sstables.insert(0, sst_id);
        } else {
            // Tiered strategy doesn't use l0.
            new_engine.levels.insert(0, (sst_id, vec![sst_id]));
        }
        new_engine.sstables.insert(sst_id, sstable);
        new_engine.imm_memtables.pop();

        let mut engine = self.state.write();
        *engine = Arc::new(new_engine);

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // Double check lock.
        let earliest_memtable = self.get_earliest_imm_memtable();
        if earliest_memtable.is_none() {
            return Ok(());
        }

        let _mutex = self.state_lock.lock();

        let earliest_memtable_ = self.get_earliest_imm_memtable();
        // If the above checked memtable is removed, do nothing
        if earliest_memtable_.is_none()
            || !Arc::ptr_eq(
                earliest_memtable.as_ref().unwrap(),
                earliest_memtable_.as_ref().unwrap(),
            )
        {
            return Ok(());
        }

        self.flush_earliest_memtable(&_mutex)?;

        Ok(())
    }

    pub fn new_txn(self: &Arc<LsmStorageInner>) -> Result<Arc<Transaction>> {
        Ok(self
            .mvcc()
            .new_txn(Arc::clone(&self), self.options.serializable))
    }

    fn to_memtables_merge_iter(
        engine: &LsmStorageState,
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
    ) -> MergeIterator<MemTableIterator> {
        let mut memtable_iters: Vec<Box<MemTableIterator>> =
            vec![Box::new(engine.memtable.scan(lower, upper))];
        memtable_iters.extend(
            engine
                .imm_memtables
                .iter()
                .map(|x| Box::new(x.scan(lower, upper))),
        );
        MergeIterator::create(memtable_iters)
    }

    fn contain_single_key(lower: &Bound<KeySlice>, upper: &Bound<KeySlice>) -> bool {
        match (lower, upper) {
            (Bound::Included(lower_key), Bound::Included(upper_key)) => lower_key == upper_key,
            _ => false,
        }
    }

    fn may_contain_key(key: &Bound<KeySlice>, sst: &Arc<SsTable>) -> bool {
        if let Bound::Included(k) = key {
            sst.may_contain(k.key_ref())
        } else {
            false
        }
    }

    fn sst_iter_seek_to_key(lower: Bound<KeySlice>, sst_iter: &mut SsTableIterator) -> Result<()> {
        match lower {
            Bound::Included(key) => sst_iter.seek_to_key(key),
            Bound::Excluded(key) => {
                let key_slice = KeySlice::from_slice(key.key_ref(), TS_RANGE_END);
                sst_iter.seek_to_key(key)?;
                while sst_iter.is_valid() && sst_iter.key().key_ref() == key.key_ref() {
                    sst_iter.next()?
                }
                Ok(())
            }
            _ => Ok(()),
        }
    }

    fn to_l0_merge_iter(
        level_id: usize,
        engine: &LsmStorageState,
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
    ) -> Result<MergeIterator<SsTableIterator>> {
        // If the range only contains exactly one key, that's a get operation, we can use blooms
        // to improve the performance.
        let is_point_lookup = Self::contain_single_key(&lower, &upper);

        let mut sstable_iters = Vec::<Box<SsTableIterator>>::new();
        let sst_ids = engine.l0_sstables.to_vec();

        for id in sst_ids {
            let table = Arc::clone(&engine.sstables[&id]);
            let may_contain = Self::may_contain_key(&lower, &table);
            let has_overlap =
                table.has_overlap(lower.map(|x| x.key_ref()), upper.map(|x| x.key_ref()));
            if is_point_lookup && !Self::may_contain_key(&lower, &table) {
                continue;
            }
            if table.has_overlap(lower.map(|x| x.key_ref()), upper.map(|x| x.key_ref())) {
                let mut iter = SsTableIterator::create_and_seek_to_first(table)?;
                Self::sst_iter_seek_to_key(lower, &mut iter)?;
                sstable_iters.push(Box::new(iter));
            }
        }

        Ok(MergeIterator::create(sstable_iters))
    }

    fn to_non_l0_concat_iter(
        state: &LsmStorageState,
        level_id: usize,
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
    ) -> Result<SstConcatIterator> {
        let level_ssts = state.levels[level_id - 1]
            .1
            .iter()
            .map(|sst_id| Arc::clone(&state.sstables[sst_id]))
            .collect::<Vec<Arc<SsTable>>>();
        let iter = match &lower {
            Bound::Included(key) | Bound::Excluded(key) => {
                SstConcatIterator::create_and_seek_to_key(level_ssts, *key)?
            }
            /*Bound::Excluded(excluded_key) => {
                let mut iter =
                    SstConcatIterator::create_and_seek_to_key(level_ssts, excluded_key.clone())?;
                while iter.is_valid() && &iter.key() <= excluded_key {
                    iter.next();
                }
                iter
            }*/
            Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(level_ssts)?,
        };
        Ok(iter)
    }

    fn to_non_l0_merge_iter(
        state: &LsmStorageState,
        lower: Bound<KeySlice>,
        upper: Bound<KeySlice>,
    ) -> Result<MergeIterator<SstConcatIterator>> {
        let mut concat_iters = Vec::<Box<SstConcatIterator>>::with_capacity(state.levels.len());
        for (level_id, sst_ids) in &state.levels {
            let concat_iter = Self::to_non_l0_concat_iter(state, *level_id, lower, upper)?;
            concat_iters.push(Box::new(concat_iter));
        }
        Ok(MergeIterator::create(concat_iters))
    }

    /// Create an iterator without skipping tombstones and duplicate keys.
    fn create_lsm_iter(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
        read_ts: u64,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        // Use the full range to get all versions of keys and filter them by ts in LsmIterator.
        let lower = lower.map(|key| KeySlice::from_slice(key, TS_RANGE_BEGIN));
        let upper = upper.map(|key| KeySlice::from_slice(key, TS_RANGE_END));

        let memtables_merge_iter = Self::to_memtables_merge_iter(&snapshot, lower, upper);
        let l0_sst_merge_iter = Self::to_l0_merge_iter(0, &snapshot, lower, upper)?;
        let non_l0_sst_merge_iter = Self::to_non_l0_merge_iter(&snapshot, lower, upper)?;

        let memtable_and_l0_merge_iter =
            TwoMergeIterator::create(memtables_merge_iter, l0_sst_merge_iter)?;
        let iter = LsmIterator::new(
            TwoMergeIterator::create(memtable_and_l0_merge_iter, non_l0_sst_merge_iter)?,
            lower.map(|key| Bytes::copy_from_slice(key.key_ref())),
            upper.map(|key| Bytes::copy_from_slice(key.key_ref())),
            read_ts,
        )?;
        Ok(FusedIterator::new(iter))
    }

    pub fn scan_with_txn(
        self: &Arc<Self>,
        txn: Arc<Transaction>,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<TxnIterator> {
        let lsm_iter = self.create_lsm_iter(lower, upper, txn.read_ts)?;
        let txn_local_iter = TxnLocalIterator::create(
            Arc::clone(&txn.local_storage),
            lower.map(Bytes::copy_from_slice),
            upper.map(Bytes::copy_from_slice),
        )?;
        let txn_and_lsm_iter = TwoMergeIterator::create(txn_local_iter, lsm_iter)?;
        TxnIterator::create(txn, txn_and_lsm_iter)
    }

    pub fn scan(self: &Arc<Self>, lower: Bound<&[u8]>, upper: Bound<&[u8]>) -> Result<TxnIterator> {
        let txn = self
            .mvcc()
            .new_txn(Arc::clone(self), self.options.serializable);
        Self::scan_with_txn(self, txn, lower, upper)
    }
}
