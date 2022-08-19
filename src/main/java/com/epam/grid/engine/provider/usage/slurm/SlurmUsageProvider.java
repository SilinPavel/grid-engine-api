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

package com.epam.grid.engine.provider.usage.slurm;

import com.epam.grid.engine.entity.CommandType;
import com.epam.grid.engine.entity.usage.UsageReport;
import com.epam.grid.engine.entity.usage.UsageReportFilter;
import com.epam.grid.engine.exception.GridEngineException;
import com.epam.grid.engine.provider.usage.UsageProvider;
import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;

@Service
@AllArgsConstructor
@Slf4j
@ConditionalOnProperty(name = "grid.engine.type", havingValue = "SLURM")
public class SlurmUsageProvider implements UsageProvider {


    @Override
    public UsageReport getUsageReport(final UsageReportFilter filter) {
        throw new GridEngineException(HttpStatus.BAD_REQUEST, "Usage report is not supported for SLURM.");
    }

    @Override
    public CommandType getProviderType() {
        return CommandType.SLURM;
    }

}
