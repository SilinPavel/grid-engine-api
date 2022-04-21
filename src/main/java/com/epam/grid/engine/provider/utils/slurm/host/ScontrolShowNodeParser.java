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

package com.epam.grid.engine.provider.utils.slurm.host;

import com.epam.grid.engine.entity.host.slurm.SlurmHost;
import com.epam.grid.engine.exception.GridEngineException;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * This class parses the result of SLURM
 * scontrol show node ID command into SlurmHost entity.
 *
 * @see com.epam.grid.engine.entity.host.slurm.SlurmHost
 */
@Component
public class ScontrolShowNodeParser {

    private static final String NODE_NAME = "NodeName";
    private static final String ARCHITECTURE = "Arch";
    private static final String CPU_TOTAL = "CPUTot";
    private static final String SOCKETS = "Sockets";
    private static final String CORES_PER_SOCKET = "CoresPerSocket";
    private static final String THREADS_PER_CORE = "ThreadsPerCore";
    private static final String REAL_MEMORY = "RealMemory";
    private static final String ALLOCATED_MEMORY = "AllocMem";
    private static final String SPACE_DELIMITER = " ";
    private static final String EQUALITY_DELIMITER = "=";
    private static final String CANT_PARSE_STDOUT_TO_SLURM_HOST = "Can't parse command result to SlurmHost";

    public SlurmHost mapHostDataToSlurmHost(final String hostData) {
        try {
            final Map<String, String> results = convertHostDataIntoMap(hostData);
            return SlurmHost.builder()
                    .nodeName(results.get(NODE_NAME))
                    .arch(results.get(ARCHITECTURE))
                    .cpuTotal(Integer.valueOf(results.get(CPU_TOTAL)))
                    .sockets(Integer.valueOf(results.get(SOCKETS)))
                    .coresPerSocket(Integer.valueOf(results.get(CORES_PER_SOCKET)))
                    .threadsPerCore(Integer.valueOf(results.get(THREADS_PER_CORE)))
                    .realMemory(Long.valueOf(results.get(REAL_MEMORY)))
                    .allocatedMemory(Long.valueOf(results.get(ALLOCATED_MEMORY)))
                    .build();
        } catch (final NumberFormatException e) {
            throw new GridEngineException(HttpStatus.INTERNAL_SERVER_ERROR,
                    CANT_PARSE_STDOUT_TO_SLURM_HOST, e);
        }
    }

    private Map<String, String> convertHostDataIntoMap(final String hostData) {
        return Arrays.stream(hostData.trim().split(SPACE_DELIMITER))
                .filter(string -> string.contains(EQUALITY_DELIMITER))
                .map(string -> string.split(EQUALITY_DELIMITER, 2))
                .collect(Collectors.toMap(array -> array[0], array -> array[1]));
    }
}
