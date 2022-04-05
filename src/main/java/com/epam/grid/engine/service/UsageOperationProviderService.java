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

package com.epam.grid.engine.service;

import com.epam.grid.engine.entity.EngineType;
import com.epam.grid.engine.entity.usage.UsageReport;
import com.epam.grid.engine.entity.usage.UsageReportFilter;
import com.epam.grid.engine.exception.GridEngineException;
import com.epam.grid.engine.provider.usage.UsageProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.List;

/**
 * The class which redirects the call from the {@link com.epam.grid.engine.controller.usage.UsageOperationController}
 * to the corresponding UsageProvider type.
 */
@Service
public class UsageOperationProviderService {

    private final EngineType engineType;
    @Autowired
    private UsageProvider usageProvider;

    public UsageOperationProviderService(@Value("${grid.engine.type}") final EngineType engineType) {
        this.engineType = engineType;
    }

    /**
     * Returns a repor`t containing usage summary information received from the corresponding
     * UsageProvider type with filters applied.
     *
     * @param filter List of keys for setting filters.
     * @return the usage report.
     */
    public UsageReport getUsageReport(final UsageReportFilter filter) {
        return getUsageProvider().getUsageReport(filter);
    }

    private UsageProvider getUsageProvider() {
        Assert.notNull(usageProvider, String.format("Provides for type '%s' is not supported", engineType));
        return usageProvider;
    }

}
