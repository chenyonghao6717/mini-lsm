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

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    // 100 * (sst num of Ln+1) / (sst num of Ln)
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        _snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        if _snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            let l1_id = 1;
            return Some(SimpleLeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: _snapshot.l0_sstables.clone(),
                lower_level: l1_id,
                lower_level_sst_ids: _snapshot.levels[l1_id - 1].1.clone(),
                is_lower_level_bottom_level: false,
            });
        }

        // From 1 to n
        let max_level_id = _snapshot.levels.len();
        for upper_level_id in 1..max_level_id {
            let upper_level = &_snapshot.levels[upper_level_id - 1];

            let lower_level_id = upper_level_id + 1;
            let lower_level = &_snapshot.levels[lower_level_id - 1];
            let is_lower_level_bottom_level = lower_level_id == max_level_id;

            let upper_sst_num = upper_level.1.len();
            let lower_sst_num = lower_level.1.len();

            let need_compact = upper_sst_num > 0
                && lower_sst_num * 100 / upper_sst_num < self.options.size_ratio_percent;

            if need_compact {
                return Some(SimpleLeveledCompactionTask {
                    upper_level: Some(upper_level.0),
                    upper_level_sst_ids: upper_level.1.clone(),
                    lower_level: lower_level.0,
                    lower_level_sst_ids: lower_level.1.clone(),
                    is_lower_level_bottom_level,
                });
            }
        }

        None
    }

    pub fn apply_compaction_result_to_l0(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut new_engine = _snapshot.clone();
        let mut consumed_sst_ids = Vec::<usize>::new();

        // New sst will be flushed while compacting, we need to keep them.
        let (consumed_l0_sst_ids, unconsumed_l0_sst_ids) = _snapshot
            .l0_sstables
            .iter()
            .cloned()
            .partition(|id| _task.upper_level_sst_ids.contains(id));

        // Collect consumed sst ids.
        consumed_sst_ids.extend(consumed_l0_sst_ids);
        let l1_id = 1;
        consumed_sst_ids.extend_from_slice(&_snapshot.levels[l1_id - 1].1);

        // Apply new l0 and l1
        new_engine.l0_sstables = unconsumed_l0_sst_ids;
        let new_l1 = (l1_id, _output.to_vec());
        new_engine.levels[l1_id - 1] = new_l1;

        (new_engine, consumed_sst_ids)
    }

    pub fn apply_compaction_result_to_l1_and_lower(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut consumed_sst_ids = Vec::<usize>::new();
        consumed_sst_ids.extend_from_slice(&_task.upper_level_sst_ids);
        consumed_sst_ids.extend_from_slice(&_task.lower_level_sst_ids);

        let upper_level_id = _task.upper_level.unwrap();
        let new_upper_level = (upper_level_id, Vec::<usize>::new());
        let new_lower_level = (_task.lower_level, _output.to_vec());

        let mut new_engine = _snapshot.clone();
        new_engine.levels[upper_level_id - 1] = new_upper_level;
        new_engine.levels[_task.lower_level - 1] = new_lower_level;

        (new_engine, consumed_sst_ids)
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        _snapshot: &LsmStorageState,
        _task: &SimpleLeveledCompactionTask,
        _output: &[usize], // Sst ids after merge. They should be put in lower level.
    ) -> (LsmStorageState, Vec<usize>) {
        if _task.upper_level.is_none() {
            self.apply_compaction_result_to_l0(_snapshot, _task, _output)
        } else {
            self.apply_compaction_result_to_l1_and_lower(_snapshot, _task, _output)
        }
    }
}
