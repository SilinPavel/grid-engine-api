/*
 *
 *  * Copyright 2022 EPAM Systems, Inc. (https://www.epam.com/)
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *     http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 *
 */

package com.epam.grid.engine.entity.job;

import lombok.Builder;
import lombok.Value;

/**
 * This class is used to assign parallel environment options. Can be used only in SLURM.
 */
@Value
@Builder
public class ParallelExecutionOptions {
    /**
     * Number of tasks to be created for the job.
     */
    int numTasks;
    /**
     * Minimum/maximum number of nodes allocated to the job.
     */
    int nodes;
    /**
     * Number of CPUs allocated per task.
     */
    int cpusPerTask;
    /**
     * Maximum number of tasks per allocated node.
     */
    int numTasksPerNode;
    /**
     * Prevents sharing of allocated nodes with other jobs. Suballocates CPUs to job steps.
     */
    boolean exclusive;
}
