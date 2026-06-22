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

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::StorageIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn collect_sstables(
        indices: &[usize],
        snapshot: Arc<LsmStorageState>,
    ) -> Result<Vec<Arc<SsTable>>> {
        indices
            .iter()
            .map(|idx| {
                snapshot
                    .sstables
                    .get(idx)
                    .ok_or_else(|| anyhow!("Sstable of idx {} not found", idx))
                    .map(Arc::clone)
            })
            .collect::<Result<Vec<_>>>()
    }

    fn get_table_merge_iter(
        indices: &[usize],
        snapshot: Arc<LsmStorageState>,
    ) -> Result<MergeIterator<SsTableIterator>> {
        let sstable_iters = Self::collect_sstables(indices, Arc::clone(&snapshot))?
            .iter()
            .map(|table| SsTableIterator::create_and_seek_to_first(Arc::clone(table)).map(Box::new))
            .collect::<Result<_>>()?;
        Ok(MergeIterator::create(sstable_iters))
    }

    fn fully_compact(
        &self,
        _task: &CompactionTask,
        snapshot: Arc<LsmStorageState>,
    ) -> Result<Vec<Arc<SsTable>>> {
        if let CompactionTask::ForceFullCompaction {
            l0_sstables,
            l1_sstables,
        } = _task
        {
            let mut two_merge_iterator = TwoMergeIterator::create(
                Self::get_table_merge_iter(l0_sstables, Arc::clone(&snapshot))?,
                Self::get_table_merge_iter(l1_sstables, Arc::clone(&snapshot))?,
            )?;
            let mut new_tables = Vec::<Arc<SsTable>>::new();
            let mut cur_builder = SsTableBuilder::new(self.options.block_size);

            while two_merge_iterator.is_valid() {
                if cur_builder.estimated_size() > self.options.target_sst_size {
                    let id = self.next_sst_id();
                    let new_table = cur_builder.build(
                        id,
                        Some(Arc::clone(&self.block_cache)),
                        self.path_of_sst(id),
                    )?;
                    new_tables.push(Arc::new(new_table));
                    cur_builder = SsTableBuilder::new(self.options.block_size);
                }
                // In week 2 day 1 we omit all deleted keys.
                let value = two_merge_iterator.value();
                if !value.is_empty() {
                    cur_builder.add(two_merge_iterator.key(), two_merge_iterator.value());
                }
                two_merge_iterator.next()?;
            }

            if cur_builder.estimated_size() > 0 {
                let id = self.next_sst_id();
                let new_table = cur_builder.build(
                    id,
                    Some(Arc::clone(&self.block_cache)),
                    self.path_of_sst(id),
                )?;
                new_tables.push(Arc::new(new_table));
            }

            Ok(new_tables)
        } else {
            Err(anyhow!(
                "Passed a non-ForceFullCompaction task to LsmStroageInner::fully_compact!"
            ))
        }
    }

    fn compact(
        &self,
        _task: &CompactionTask,
        snapshot: Arc<LsmStorageState>,
    ) -> Result<Vec<Arc<SsTable>>> {
        match _task {
            CompactionTask::ForceFullCompaction { .. } => {
                self.fully_compact(_task, Arc::clone(&snapshot))
            }
            _ => unimplemented!(),
        }
    }

    /// Only called by one backgroud thread at any time so we don't need a mutex here.
    /// In week 2 day 1 we focus on ForceFullCompaction.
    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: snapshot.l0_sstables.to_vec(),
            l1_sstables: snapshot
                .levels
                .iter()
                .find(|level| level.0 == 1)
                .map(|level| level.1.to_vec())
                // All LsmTrees are generated with level 1
                .unwrap(),
        };

        // Replace with the new engine
        let new_l1_tables = self.compact(&task, Arc::clone(&snapshot))?;
        let state_lock = self.state_lock.lock();
        let mut new_engine = {
            let guard = self.state.read();
            (**guard).clone()
        };

        // Drop old tables
        for idx in &snapshot.l0_sstables {
            new_engine.sstables.remove(idx);
        }
        let l1_sstables = snapshot
            .levels
            .iter()
            .find(|level| level.0 == 1)
            .map(|level| level.1.to_vec())
            .unwrap();
        for idx in l1_sstables {
            new_engine.sstables.remove(&idx);
        }

        // Update table ids.
        // During merging, new l0 tables might be added, them should be kept.
        new_engine
            .l0_sstables
            .retain(|idx| !snapshot.l0_sstables.contains(idx));
        new_engine.levels.retain(|level| level.0 != 1);
        let l1_sstables = new_l1_tables
            .iter()
            .map(|table| table.sst_id())
            .collect::<Vec<usize>>();
        new_engine.levels.insert(0, (1, l1_sstables));

        // Add new tables into the new engine.
        for table in new_l1_tables {
            new_engine.sstables.insert(table.sst_id(), table);
        }

        let mut guard = self.state.write();
        *guard = Arc::new(new_engine);

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let mutable_memtable_num = 1;
        if self.options.num_memtable_limit < snapshot.imm_memtables.len() + mutable_memtable_num {
            self.force_flush_next_imm_memtable()?;
        }

        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
