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
import com.epam.grid.engine.entity.HostFilter;
import com.epam.grid.engine.entity.Listing;
import com.epam.grid.engine.entity.host.Host;
import com.epam.grid.engine.exception.GridEngineException;
import com.epam.grid.engine.provider.host.HostProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import java.util.List;

/**
 * This class determines which of the grid engines shall be used and calls appropriate methods.
 */
@Service
public class HostOperationProviderService {

    private final EngineType engineType;

    private HostProvider hostProvider;

    /**
     * This method sets grid engine type in the context.
     *
     * @param engineType type of grid engine
     * @see EngineType
     */
    public HostOperationProviderService(@Value("${grid.engine.type}") final EngineType engineType) {
        this.engineType = engineType;
    }

    /**
     * This method processes the request to provider and returns listing of hosts,
     * if request is empty it will return all the hosts.
     *
     * @param filter names of hosts needed
     * @return {@link Listing} of {@link Host}
     * @see HostFilter
     */
    public Listing<Host> filter(final HostFilter filter) {
        return getProvider().listHosts(filter);
    }

    /**
     * This method finds among all created {@link HostProvider} beans the appropriate one and sets
     * it to the corresponding field.
     *
     * @param providers list of HostProvider.
     * @see HostProvider
     */
    @Autowired
    public void setProviders(final List<HostProvider> providers) {
        hostProvider = providers.stream()
                .filter(s -> s.getProviderType().equals(engineType))
                .findAny()
                .orElseThrow(() -> new GridEngineException(HttpStatus.INTERNAL_SERVER_ERROR,
                        "Host Provider was not found"));
    }

    private HostProvider getProvider() {
        Assert.notNull(hostProvider, String.format("Provides for type '%s' is not supported", engineType));
        return hostProvider;
    }
}
