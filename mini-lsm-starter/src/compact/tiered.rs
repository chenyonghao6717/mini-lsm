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

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    // 100 * (num of sst of non-last tier) / (num of sst of last tier)
    pub max_size_amplification_percent: usize,
    // The used raito is (100 + size_ratio) %, e.g., size_ratio = 1, used ratio is 101%.
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    // Merge all non-last tier sstabls with the last tier sstables.
    fn generate_ampilification_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        let all_sst_num = snapshot
            .levels
            .iter()
            .map(|tier| tier.1.len())
            .sum::<usize>();
        let last_tier_sst_num = snapshot.levels.last().as_ref().unwrap().1.len();
        let non_last_iter_sst_num = all_sst_num - last_tier_sst_num;

        let amplification_ratio_to_large = last_tier_sst_num == 0
            || 100 * non_last_iter_sst_num / last_tier_sst_num
                >= self.options.max_size_amplification_percent;
        if amplification_ratio_to_large {
            Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            })
        } else {
            None
        }
    }

    fn generate_size_ratio_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        let mut previous_sstable_count = 0;
        for (previous_tier_count, (tier_id, tier)) in snapshot.levels.iter().enumerate() {
            if previous_tier_count >= self.options.min_merge_width && previous_sstable_count > 0 {
                let ratio = tier.len() * 100 / previous_sstable_count;
                if ratio > (100 + self.options.size_ratio) {
                    return Some(TieredCompactionTask {
                        tiers: snapshot.levels[..previous_tier_count].to_vec(),
                        // Always false because only previous tiers of this tier are going to be merged.
                        bottom_tier_included: false,
                    });
                }
            }
            previous_sstable_count += tier.len();
        }
        None
    }

    fn generate_reduce_tier_num_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        let tier_num = snapshot.levels.len();
        let num_tier = self.options.num_tiers;
        if tier_num >= num_tier {
            Some(TieredCompactionTask {
                tiers: snapshot.levels[..num_tier].to_vec(),
                bottom_tier_included: tier_num == num_tier,
            })
        } else {
            None
        }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() <= 1 || snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        let amplification_task = self.generate_ampilification_compaction_task(snapshot);
        if amplification_task.is_some() {
            return amplification_task;
        }

        let size_ratio_task = self.generate_size_ratio_compaction_task(snapshot);
        if size_ratio_task.is_some() {
            return size_ratio_task;
        }

        self.generate_reduce_tier_num_compaction_task(snapshot)
    }

    /// Ensures the id(level.0) of levels[n] is always n + 1(levels stored here begin with level id 1)
    fn rebuild_levels(levels: Vec<(usize, Vec<usize>)>) -> Vec<(usize, Vec<usize>)> {
        levels
            .into_iter()
            .enumerate()
            .map(|(idx, (_, sst_ids))| {
                let level_id = idx + 1;
                (level_id, sst_ids)
            })
            .collect()
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let sst_ids_to_remove = task
            .tiers
            .iter()
            .flat_map(|(_, tier)| tier.to_vec())
            .collect::<Vec<usize>>();

        let mut new_engine = snapshot.clone();
        let tier_num_to_remove = task.tiers.len();
        new_engine.levels = new_engine.levels[tier_num_to_remove..].to_vec();
        let new_tier = (output[0], output.to_vec());
        // Only tiers starting from the head of levels will be compacted. So the merged tier need also be put in the head.
        new_engine.levels.insert(0, new_tier);
        new_engine.levels = Self::rebuild_levels(new_engine.levels);

        (new_engine, sst_ids_to_remove)
    }
}
