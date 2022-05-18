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

package com.epam.grid.engine.provider.job.slurm;

import com.epam.grid.engine.cmd.GridEngineCommandCompiler;
import com.epam.grid.engine.cmd.SimpleCmdExecutor;
import com.epam.grid.engine.entity.CommandResult;
import com.epam.grid.engine.entity.EngineType;
import com.epam.grid.engine.entity.JobFilter;
import com.epam.grid.engine.entity.Listing;
import com.epam.grid.engine.entity.job.DeleteJobFilter;
import com.epam.grid.engine.entity.job.DeletedJobInfo;
import com.epam.grid.engine.entity.job.Job;
import com.epam.grid.engine.entity.job.JobOptions;
import com.epam.grid.engine.entity.job.JobLogInfo;
import com.epam.grid.engine.mapper.job.slurm.SlurmJobMapper;
import com.epam.grid.engine.provider.job.JobProvider;

import com.epam.grid.engine.provider.utils.CommandsUtils;
import com.epam.grid.engine.provider.utils.slurm.job.SacctCommandParser;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import org.thymeleaf.context.Context;

import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;


/**
 * This class performs various actions with jobs for the SLURM engine.
 */
@Service
@Slf4j
@ConditionalOnProperty(name = "grid.engine.type", havingValue = "SLURM")
public class SlurmJobProvider implements JobProvider {
    private static final String JOB_FILTER = "filter";
    private static final String SQUEUE_COMMAND = "squeue";

    /**
     * The MapStruct mapping mechanism used.
     */
    private final SlurmJobMapper jobMapper;

    /**
     * The command execution mechanism used.
     */
    private final SimpleCmdExecutor simpleCmdExecutor;

    private final int fieldsCount;

    /**
     * An object that forms the structure of an executable command according to a template.
     */
    private final GridEngineCommandCompiler commandCompiler;

    public SlurmJobProvider(final SlurmJobMapper jobMapper,
                            final SimpleCmdExecutor simpleCmdExecutor,
                            final GridEngineCommandCompiler commandCompiler,
                            @Value("${slurm.job.output-fields-count}") final int fieldsCount) {
        this.jobMapper = jobMapper;
        this.simpleCmdExecutor = simpleCmdExecutor;
        this.commandCompiler = commandCompiler;
        this.fieldsCount = fieldsCount;
    }

    @Override
    public EngineType getProviderType() {
        return EngineType.SLURM;
    }

    @Override
    public Listing<Job> filterJobs(final JobFilter jobFilter) {
        final CommandResult result = simpleCmdExecutor.execute(makeSqueueCommand(jobFilter));
        if (result.getExitCode() != 0) {
            CommandsUtils.throwExecutionDetails(result);
        } else if (!result.getStdErr().isEmpty()) {
            log.warn(result.getStdErr().toString());
        }
        return mapToJobListing(result.getStdOut());
    }

    /**
     * Creates the structure of an executable command based on the passed filter.
     *
     * @param filter User-defined filter for selecting job data.
     * @return The structure of an executable command.
     */
    private String[] makeSqueueCommand(final JobFilter filter) {
        final Context context = new Context();
        context.setVariable(JOB_FILTER, filter);
        return commandCompiler.compileCommand(getProviderType(), SQUEUE_COMMAND, context);
    }

    private Listing<Job> mapToJobListing(final List<String> stdOut) {
        if (stdOut.size() > 1) {
            return new Listing<>(stdOut.stream()
                    .skip(1)
                    .map(jobDataList -> SacctCommandParser.parseSlurmJob(jobDataList, fieldsCount))
                    .filter(parsedJobList -> !CollectionUtils.isEmpty(parsedJobList))
                    .map(SacctCommandParser::mapJobDataToSlurmJob)
                    .map(jobMapper::slurmJobToJob)
                    .collect(Collectors.toList()));
        }
        return new Listing<>();
    }

    @Override
    public Job runJob(final JobOptions options) {
        throw new UnsupportedOperationException("Run job operation haven't implemented yet");
    }

    @Override
    public DeletedJobInfo deleteJob(final DeleteJobFilter deleteJobFilter) {
        throw new UnsupportedOperationException("Job deletion operation haven't implemented yet");
    }

    @Override
    public JobLogInfo getJobLogInfo(final int jobId, final JobLogInfo.Type logType, final int lines,
                                    final boolean fromHead) {
        throw new UnsupportedOperationException("Job log info retrieving operation haven't implemented yet");
    }

    @Override
    public InputStream getJobLogFile(final int jobId, final JobLogInfo.Type logType) {
        throw new UnsupportedOperationException("Job log info file retrieving operation haven't implemented yet");
    }

}
