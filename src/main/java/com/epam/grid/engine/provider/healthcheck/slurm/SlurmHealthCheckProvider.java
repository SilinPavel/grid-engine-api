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

package com.epam.grid.engine.provider.healthcheck.slurm;

import com.epam.grid.engine.cmd.GridEngineCommandCompiler;
import com.epam.grid.engine.cmd.SimpleCmdExecutor;
import com.epam.grid.engine.entity.CommandResult;
import com.epam.grid.engine.entity.EngineType;
import com.epam.grid.engine.entity.healthcheck.HealthCheckInfo;
import com.epam.grid.engine.provider.healthcheck.HealthCheckProvider;
import com.epam.grid.engine.provider.utils.slurm.healthcheck.ShowConfigCommandParser;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.thymeleaf.context.Context;

/**
 * Provider class that addressing a request
 * to the SLURM Grid Engine and processes the response received.
 */
@Service
public class SlurmHealthCheckProvider implements HealthCheckProvider {

    private static final String SHOWCONFIG_COMMAND = "showConfig";

    private final SimpleCmdExecutor simpleCmdExecutor;
    private final GridEngineCommandCompiler commandCompiler;

    public SlurmHealthCheckProvider(
            final SimpleCmdExecutor simpleCmdExecutor,
            final GridEngineCommandCompiler commandCompiler
    ) {
        this.simpleCmdExecutor = simpleCmdExecutor;
        this.commandCompiler = commandCompiler;
    }

    /**
     * This method tells which grid engine is used.
     *
     * @return Type of grid engine
     * @see EngineType
     */
    @Override
    public EngineType getProviderType() {
        return EngineType.SLURM;
    }

    /**
     * This method accesses the grid engine after receiving health check request
     * and returns working grid engine status information.
     *
     * @return {@link HealthCheckInfo}
     */
    @Override
    public HealthCheckInfo checkHealth() {
        return executeShowConfigCommand();
    }

    private HealthCheckInfo executeShowConfigCommand() {
        final CommandResult result = simpleCmdExecutor.execute(getShowConfigCommand());
        return ShowConfigCommandParser.parseShowConfigResult(result);
    }

    private String[] getShowConfigCommand() {
        final Context context = new Context();
        return commandCompiler.compileCommand(getProviderType(), SHOWCONFIG_COMMAND, context);
    }

}
