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

package com.epam.grid.engine.provider.queue.slurm;

import com.epam.grid.engine.cmd.GridEngineCommandCompiler;
import com.epam.grid.engine.cmd.SimpleCmdExecutor;
import com.epam.grid.engine.entity.CommandResult;
import com.epam.grid.engine.entity.CommandType;
import com.epam.grid.engine.entity.QueueFilter;
import com.epam.grid.engine.entity.queue.Queue;
import com.epam.grid.engine.entity.queue.QueueVO;
import com.epam.grid.engine.mapper.queue.sge.SgeQueueMapper;
import com.epam.grid.engine.provider.queue.QueueProvider;
import com.epam.grid.engine.provider.utils.CommandsUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.thymeleaf.context.Context;

import java.util.List;

import static com.epam.grid.engine.utils.TextConstants.COMMA;


/**
 * A service class, that incorporates business logic, connected with Slurm Grid Engine queue.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "grid.engine.type", havingValue = "SLURM")
public class SlurmQueueProvider implements QueueProvider {
    private final String SCONTROL_COMMAND = "scontrolCommand";
    private final String SCONTROL_CREATE_COMMAND = "create";
    private final String SCONTROL_LIST_COMMAND = "";
    private final String SCONTROL_UPDATE_COMMAND = "update";
    private final String SCONTROL_DELETE_COMMAND = "delete";

    private final String SCONTROL_ACTION = "command";
    private final String SCONTROL_USER_GROUPS = "allowedUserGroups";
    private final String SCONTROL_HOSTS = "hostList";
    private final String SCONTROL_NAME = "name";
    private final String SCONTROL_OWNERS = "ownerList";

    /**
     * The MapStruct mapping mechanism used.
     */
    private final SgeQueueMapper queueMapper;

    private final SimpleCmdExecutor simpleCmdExecutor;

    /**
     * An object that forms the structure of an executable command according to a template.
     */
    private final GridEngineCommandCompiler commandCompiler;

    /**
     * Returns Slurm Grid Engine.
     *
     * @return current engine type - Slurm Grid Engine
     */
    @Override
    public CommandType getProviderType() {
        return CommandType.SLURM;
    }

    @Override
    public Queue registerQueue(final QueueVO registrationRequest) {
        if (registrationRequest.getParallelEnvironmentNames() != null) {
            throw new UnsupportedOperationException("Parallel environment variables cannot be used in Slurm!");
        }
        if (registrationRequest.getName() == null) {
            throw new UnsupportedOperationException("Partition name option is obligatory");
        }
        final Context context = prepareContext(registrationRequest);
        final CommandResult result = simpleCmdExecutor.execute(commandCompiler.compileCommand(getProviderType(), SCONTROL_COMMAND, context));
        if (result.getExitCode() != 0 || result.getStdOut().isEmpty()) {
            CommandsUtils.throwExecutionDetails(result, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        return Queue.builder()
                .name(registrationRequest.getName())
                .ownerList(registrationRequest.getOwnerList())
                .build();
    }

    @Override
    public Queue updateQueue(QueueVO updateRequest) {
        return null;
    }

    @Override
    public List<Queue> listQueues() {
        return null;
    }

    @Override
    public List<Queue> listQueues(QueueFilter queueFilter) {
        return null;
    }

    @Override
    public Queue deleteQueues(String queueName) {
        return null;
    }

    private Context prepareContext(final QueueVO registrationRequest) {
        final Context context = new Context();
        context.setVariable(SCONTROL_ACTION, SCONTROL_CREATE_COMMAND);
        if (registrationRequest.getAllowedUserGroups() != null) {
            context.setVariable(SCONTROL_USER_GROUPS, String.join(COMMA, registrationRequest.getAllowedUserGroups()));
        }
        if (registrationRequest.getHostList() != null) {
            context.setVariable(SCONTROL_HOSTS, String.join(COMMA, registrationRequest.getHostList()));
        }
        context.setVariable(SCONTROL_NAME, registrationRequest.getName());
        return context;
    }

}
