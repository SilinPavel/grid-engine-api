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
import com.epam.grid.engine.entity.queue.SlotsDescription;
import com.epam.grid.engine.exception.GridEngineException;
import com.epam.grid.engine.provider.queue.QueueProvider;
import com.epam.grid.engine.provider.utils.CommandsUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.thymeleaf.context.Context;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.epam.grid.engine.utils.TextConstants.COMMA;


/**
 * A service class, that incorporates business logic, connected with Slurm Grid Engine queue.
 */
@Slf4j
@Service
@RequiredArgsConstructor
@ConditionalOnProperty(name = "grid.engine.type", havingValue = "SLURM")
public class SlurmQueueProvider implements QueueProvider {
    private static final String SCONTROL_COMMAND = "scontrolCommand";
    private static final String SINFO_COMMAND = "sinfo";
    private static final String SCONTROL_CREATE_COMMAND = "create";
    private static final String SCONTROL_UPDATE_COMMAND = "update";
    private static final String SCONTROL_DELETE_COMMAND = "delete";

    private static final String SCONTROL_ACTION = "command";
    private static final String SCONTROL_USER_GROUPS = "allowedUserGroups";
    private static final String SCONTROL_HOSTS = "hostList";
    private static final String SCONTROL_PARTITION_NAME = "partitionName";
    private static final String STANDARD_SLURM_DELIMETER = "\\|";
    private static final String LEFT_SQUARE_BRACKET_SIGN = "\\[";
    private static final String HYPHEN_SIGN = "-";

    private static final Pattern NODES_RANGE_REGEX = Pattern.compile("[a-zA-Z]+\\[\\d+-\\d+]");
    private static final int SCONTROL_OUPUT_SIZE = 42;
    private static final int SCONTROL_OUPUT_PARTNAME_INDEX = 33;
    private static final int SCONTROL_OUPUT_NODES_INDEX = 31;
    private static final int SCONTROL_OUPUT_USERGROUPS_INDEX = 6;
    private static final int SCONTROL_OUPUT_CPUS_INDEX = 2;

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
        checkRegistrationRequest(registrationRequest);
        final Context context = prepareContext(registrationRequest);
        final CommandResult result = simpleCmdExecutor.execute(commandCompiler.compileCommand(getProviderType(),
                SCONTROL_COMMAND, context));
        checkIsResultIsCorrect(result);
        return Queue.builder()
                .name(registrationRequest.getName())
                .hostList(registrationRequest.getHostList())
                .allowedUserGroups(registrationRequest.getAllowedUserGroups())
                .slots(new SlotsDescription(2, Collections.emptyMap()))
                .build();
    }

    @Override
    public Queue updateQueue(final QueueVO updateRequest) {
        checkRegistrationRequest(updateRequest);

        final String updateName = updateRequest.getName();
        final List<String> updateHostList = updateRequest.getHostList();
        final List<String> updateUserGroups = updateRequest.getAllowedUserGroups();
        if (updateName == null || updateHostList == null || updateUserGroups == null) {
            throw new UnsupportedOperationException("Name, hostList and allowedUserGroups should be specified for "
                    + "successful partition update");
        }

        final Context context = createContextWithUpdatePartitionName(updateName);
        final CommandResult sinfoResult = simpleCmdExecutor.execute(commandCompiler.compileCommand(getProviderType(),
                SINFO_COMMAND, context));
        checkIfExecutionResultIsEmpty(sinfoResult);

        final List<List<String>> partitionData = getNodesData(sinfoResult.getStdOut());

        final List<String> currentNodesDecrypted = getNodesFromPartitionDataAndDecrypt(partitionData);
        final List<String> userGroups = getUserGroupsFromPartitionData(partitionData);
        final int cpus = getCpusFromPartitionData(partitionData);

        final List<String> updateHostListDecrypted = decryptGroupOfNodes(updateHostList);
        sortList(updateUserGroups);

        if (updateHostListDecrypted.equals(currentNodesDecrypted) && updateUserGroups.equals(userGroups)) {
            throw new GridEngineException(HttpStatus.BAD_REQUEST, "New partition properties and the current one are "
                    + "equal");
        }
        fillContextWithDateToUpdate(context, updateUserGroups, updateHostListDecrypted);

        final CommandResult result = simpleCmdExecutor.execute(commandCompiler.compileCommand(getProviderType(),
                SCONTROL_COMMAND, context));
        checkIsResultIsCorrect(result);

        return Queue.builder()
                .name(updateName)
                .hostList(updateHostListDecrypted)
                .allowedUserGroups(updateUserGroups)
                .slots(new SlotsDescription(cpus, Collections.emptyMap()))
                .build();
    }

    @Override
    public List<Queue> listQueues() {
        final CommandResult result = simpleCmdExecutor.execute(commandCompiler.compileCommand(getProviderType(),
                SINFO_COMMAND, new Context()));
        checkIsResultIsCorrect(result);

        if (result.getStdOut().size() == 0) {
            return Collections.EMPTY_LIST;
        }

        return fillQueueNameFromOutput(result.getStdOut());
    }

    @Override
    public List<Queue> listQueues(final QueueFilter queueFilter) {
        final Context context = new Context();
        if (queueFilter.getQueues() != null) {
            context.setVariable(SCONTROL_PARTITION_NAME, String.join(COMMA, queueFilter.getQueues()));
        }
        final CommandResult result = simpleCmdExecutor.execute(commandCompiler.compileCommand(getProviderType(),
                SINFO_COMMAND, context));
        checkIsResultIsCorrect(result);

        if (result.getStdOut().size() == 0) {
            return Collections.EMPTY_LIST;
        }

        return fillQueueData(result.getStdOut());
    }

    @Override
    public Queue deleteQueues(final String queueName) {
        if (queueName == null) {
            throw new UnsupportedOperationException("Partition name for deletion should be specified");
        }
        final Context context = new Context();
        context.setVariable(SCONTROL_ACTION, SCONTROL_DELETE_COMMAND);
        context.setVariable(SCONTROL_PARTITION_NAME, queueName);
        final CommandResult result = simpleCmdExecutor.execute(commandCompiler.compileCommand(getProviderType(),
                SCONTROL_COMMAND, context));
        checkIsResultIsCorrect(result);
        return Queue.builder()
                .name(queueName)
                .build();
    }

    private List<String> getNodesFromPartitionDataAndDecrypt(final List<List<String>> partitionData) {
        final List<String> nodesData = partitionData.stream()
                .map(nodeData -> nodeData.get(1))
                .distinct()
                .sorted(String::compareTo)
                .collect(Collectors.toList());
        return decryptGroupOfNodes(nodesData);
    }

    private List<String> getUserGroupsFromPartitionData(final List<List<String>> partitionData) {
        return partitionData.stream()
                .map(nodeData -> nodeData.get(2))
                .distinct()
                .map(String::toUpperCase)
                .sorted(String::compareTo)
                .collect(Collectors.toList());
    }

    private int getCpusFromPartitionData(final List<List<String>> partitionData) {
        return partitionData.stream()
                .mapToInt(nodeData -> nodeData.get(3) != null
                        ? Integer.parseInt(nodeData.get(3))
                        : 0)
                .sum();
    }

    private void sortList(final List<String> list) {
        list.sort(String::compareTo);
    }

    private List<String> decryptGroupOfNodes(final List<String> updateHostList) {
        final List<String> decryptedNodes = new ArrayList<>();
        updateHostList
                .forEach(host -> {
                    final Matcher matcher = NODES_RANGE_REGEX.matcher(host);
                    if (matcher.find()) {
                        decryptedNodes.addAll(expandHosts(host));
                    } else {
                        decryptedNodes.add(host.trim());
                    }
                });
        sortList(decryptedNodes);
        return decryptedNodes;
    }

    private void checkIsResultIsCorrect(final CommandResult result) {
        if (result.getExitCode() != 0 || !result.getStdErr().isEmpty()) {
            CommandsUtils.throwExecutionDetails(result, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }

    private void fillContextWithDateToUpdate(final Context context, final List<String> updateUserGroups,
                                             final List<String> updateHostList) {
        context.setVariable(SCONTROL_ACTION, SCONTROL_UPDATE_COMMAND);
        context.setVariable(SCONTROL_USER_GROUPS, String.join(COMMA, updateUserGroups));
        context.setVariable(SCONTROL_HOSTS, String.join(COMMA, updateHostList));
    }

    private Context createContextWithUpdatePartitionName(final String updateName) {
        final Context context = new Context();
        context.setVariable(SCONTROL_PARTITION_NAME, updateName);
        return context;
    }

    private void checkIfExecutionResultIsEmpty(final CommandResult executionResult) {
        if (executionResult.getExitCode() != 0 || !executionResult.getStdErr().isEmpty()
                || executionResult.getStdOut().isEmpty()) {
            throw new GridEngineException(HttpStatus.BAD_REQUEST, "Failed to fetch partition data. Check partition "
                    + "name. " + executionResult);
        }
    }

    private List<List<String>> getNodesData(final List<String> resultOutput) {
        return resultOutput.stream()
                .map(nodeData -> {
                    final String[] nodeDataArray = nodeData.split(STANDARD_SLURM_DELIMETER);
                    if (nodeDataArray.length != SCONTROL_OUPUT_SIZE) {
                        throw new GridEngineException(HttpStatus.NOT_FOUND, "Node data is inconsistent: waiting for "
                                + SCONTROL_OUPUT_SIZE + " fields, but " + nodeDataArray.length + " were fetched.");
                    }
                    return nodeDataArray;
                })
                .map(nodeDataArray -> List.of(
                        nodeDataArray[SCONTROL_OUPUT_PARTNAME_INDEX],
                        nodeDataArray[SCONTROL_OUPUT_NODES_INDEX],
                        nodeDataArray[SCONTROL_OUPUT_USERGROUPS_INDEX],
                        nodeDataArray[SCONTROL_OUPUT_CPUS_INDEX]
                ))
                .collect(Collectors.toList());
    }

    private List<Queue> fillQueueNameFromOutput(final List<String> resultOutput) {
        return resultOutput.stream()
                .map(nodeData -> {
                    final String[] nodeDataArray = nodeData.split(STANDARD_SLURM_DELIMETER);
                    if (nodeDataArray.length != SCONTROL_OUPUT_SIZE) {
                        throw new GridEngineException(HttpStatus.NOT_FOUND, "Node ouput data is incorrect");
                    }
                    return nodeDataArray[SCONTROL_OUPUT_PARTNAME_INDEX].trim();
                })
                .distinct()
                .map(partitionName -> Queue.builder()
                        .name(partitionName)
                        .build())
                .collect(Collectors.toList());
    }

    private List<Queue> fillQueueData(final List<String> resultOutput) {
        final List<List<String>> requiredNodeDataList = resultOutput.stream()
                .map(nodeData -> {
                    final String[] nodeDataArray = nodeData.split(STANDARD_SLURM_DELIMETER);
                    if (nodeDataArray.length != SCONTROL_OUPUT_SIZE) {
                        throw new GridEngineException(HttpStatus.NOT_FOUND, "Node output data is incorrect");
                    }
                    return getPartitionDataFromFilteringOutput(nodeDataArray);
                })
                .distinct()
                .collect(Collectors.toList());
        final Map<String, List<String>> partitionsDataMap = groupNodesByPartitions(requiredNodeDataList);
        return partitionsDataMap.entrySet().stream().map(
                        partitionDataEntry -> {
                            final List<String> partitionDataEntryValue = partitionDataEntry.getValue();
                            return Queue.builder()
                                    .name(partitionDataEntry.getKey())
                                    .hostList(List.of(partitionDataEntryValue.get(0).split(COMMA)))
                                    .allowedUserGroups(List.of(partitionDataEntryValue.get(1).split(COMMA)))
                                    .slots(new SlotsDescription(Integer.parseInt(partitionDataEntryValue.get(2)),
                                            Collections.emptyMap()))
                                    .build();
                        })
                .collect(Collectors.toList());
    }

    private Map<String, List<String>> groupNodesByPartitions(final List<List<String>> nodesData) {
        final Map<String, List<String>> partitionsMap = new HashMap<>();
        nodesData.forEach(nodeData -> {
            final List<String> currentNodeData = partitionsMap.get(nodeData.get(0));
            if (currentNodeData != null) {
                currentNodeData.set(0, currentNodeData.get(0).concat(",").concat(nodeData.get(1).trim()));
                currentNodeData.set(2, String.valueOf(
                        Integer.parseInt(currentNodeData.get(2)) + Integer.parseInt(nodeData.get(3)))
                );
            } else {
                partitionsMap.put(nodeData.get(0),
                        Arrays.asList(nodeData.get(1).trim(), nodeData.get(2), nodeData.get(3)));
            }
        });
        return partitionsMap;
    }

    private List<String> getPartitionDataFromFilteringOutput(final String[] output) {
        final List<String> partitionDataList = new ArrayList<>();
        partitionDataList.add(output[SCONTROL_OUPUT_PARTNAME_INDEX].trim());
        partitionDataList.add(output[SCONTROL_OUPUT_NODES_INDEX]);
        partitionDataList.add(output[SCONTROL_OUPUT_USERGROUPS_INDEX]);
        partitionDataList.add(output[SCONTROL_OUPUT_CPUS_INDEX]);
        return partitionDataList;
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
        context.setVariable(SCONTROL_PARTITION_NAME, registrationRequest.getName());
        return context;
    }

    private List<String> expandHosts(final String hosts) {
        final String[] hostsArray = hosts.split(LEFT_SQUARE_BRACKET_SIGN);
        final String hostName = hostsArray[0];
        final String[] rangeArray = hostsArray[1].split(HYPHEN_SIGN);
        final int fromInt = Integer.parseInt(rangeArray[0]);
        final int toInt = Integer.parseInt(rangeArray[1].substring(0, rangeArray[1].length() - 1));
        return IntStream.rangeClosed(fromInt, toInt)
                .mapToObj(hostNumber -> hostName.concat(String.valueOf(hostNumber))).collect(Collectors.toList());
    }

    private void checkRegistrationRequest(final QueueVO registrationRequest) {
        if (registrationRequest.getName() == null) {
            throw new UnsupportedOperationException("Partition name option is obligatory");
        }
        if (registrationRequest.getParallelEnvironmentNames() != null) {
            throw new UnsupportedOperationException("Parallel environment variables cannot be used in Slurm!");
        }
        if (registrationRequest.getOwnerList() != null) {
            throw new UnsupportedOperationException("Owners cannot be set for partitions in slurm");
        }
    }
}
