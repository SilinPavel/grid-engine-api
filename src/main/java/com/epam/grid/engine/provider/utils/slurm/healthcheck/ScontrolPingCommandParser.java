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

package com.epam.grid.engine.provider.utils.slurm.healthcheck;

import com.epam.grid.engine.entity.CommandResult;
import com.epam.grid.engine.entity.healthcheck.GridEngineStatus;
import com.epam.grid.engine.entity.healthcheck.HealthCheckInfo;
import com.epam.grid.engine.entity.healthcheck.StatusInfo;
import com.epam.grid.engine.exception.GridEngineException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.http.HttpStatus;

import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Date;
import java.util.List;

import static com.epam.grid.engine.utils.TextConstants.SPACE;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ScontrolPingCommandParser {

    private static final String UP_STATE = "UP";
    private static final String DOWN_STATE = "DOWN";

    private static final String CANT_FIND_CONNECTION = "can't find connection";
    private static final String CANT_FIND_CONNECTION_MESSAGE = "Can`t find connection via specified port";
    private static final String DATE_FORMAT_CHECK_TIME = "MM/dd/yyyy HH:mm:ss:";
    private static final String DATE_FORMAT_START_TIME = "MM/dd/yyyy HH:mm:ss";
    private static final Long NOT_PROVIDED = 99999L;
    private static final String DOUBLED_SPACES_REGEX = "\\s+";

    public static HealthCheckInfo parseScontrolPingResult(final CommandResult commandResult) {
        validateScontrolPingResponse(commandResult);
        final List<String> stdOut = commandResult.getStdOut();
        final StatusInfo statusInfo = parseStatusInfo(stdOut);
        return HealthCheckInfo.builder()
                .statusInfo(statusInfo)
                .startTime(getStartTime())
                .checkTime(getCheckTime())
                .build();
    }

    private static void validateScontrolPingResponse(final CommandResult commandResult) {
        if (CollectionUtils.isEmpty(commandResult.getStdOut())) {
            throw new GridEngineException(HttpStatus.NOT_FOUND,
                    String.format("Slurm error during health check. %nexitCode = %d %nstdOut: %s %nstdErr: %s",
                            commandResult.getExitCode(), commandResult.getStdOut(), commandResult.getStdErr())
            );
        }
        if (checkCantFindConnectionCase(commandResult)) {
            throw new GridEngineException(HttpStatus.NOT_FOUND, CANT_FIND_CONNECTION_MESSAGE);
        }
    }

    private static boolean checkCantFindConnectionCase(final CommandResult commandResult) {
        return commandResult.getExitCode() != 0
                && commandResult.getStdOut().get(0).contains(CANT_FIND_CONNECTION)
                && !commandResult.getStdErr().isEmpty();
    }

    private static StatusInfo parseStatusInfo(final List<String> stdOut) {
        final long statusCode = parseStatusCode(stdOut);
        return StatusInfo.builder()
                .code(statusCode)
                .status(parseStatus(statusCode))
                .info(parseInfo(stdOut))
                .build();
    }

    private static long parseStatusCode(final List<String> stdOut) {
        long statusCode = NOT_PROVIDED;
        final List<String> statusString = List.of(stdOut.get(0).replaceAll(DOUBLED_SPACES_REGEX, SPACE).split(SPACE));
        if (statusString.size() > 0) {
            final String status = statusString.get(statusString.size() - 1);
            switch (status) {
                case UP_STATE:
                    statusCode = 0L;
                    break;
                case DOWN_STATE:
                    statusCode = 2L;
                    break;
            }
        }

        return statusCode;
    }

    private static GridEngineStatus parseStatus(final long status) {
        return GridEngineStatus.getById(status)
                .orElseThrow(() -> new GridEngineException(HttpStatus.INTERNAL_SERVER_ERROR,
                        "Not valid status value provided: " + status));
    }

    private static String parseInfo(final List<String> stdOut) {
        return stdOut.stream().filter(out -> out.contains(UP_STATE) || out.contains(DOWN_STATE)).findAny()
                .map(infoString -> infoString.replaceAll(DOUBLED_SPACES_REGEX, SPACE))
                .map(String::trim)
                .orElseThrow(() -> new GridEngineException(HttpStatus.INTERNAL_SERVER_ERROR,
                        "unable to find server status in stdOut, stdOut: " + stdOut));
    }

    private static LocalDateTime getCheckTime() {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT_CHECK_TIME);
        return tryParseStringToLocalDateTime(new SimpleDateFormat(DATE_FORMAT_CHECK_TIME)
                        .format(new Date(0)),
                formatter);
    }

    private static LocalDateTime getStartTime() {
        return tryParseStringToLocalDateTime(new SimpleDateFormat(DATE_FORMAT_START_TIME).format(new Date()),
                DateTimeFormatter.ofPattern(DATE_FORMAT_START_TIME));
    }

    private static LocalDateTime tryParseStringToLocalDateTime(
            final String dateString,
            final DateTimeFormatter formatter
    ) {
        try {
            return LocalDateTime.parse(dateString, formatter);
        } catch (final DateTimeParseException dateTimeParseException) {
            throw new GridEngineException(HttpStatus.INTERNAL_SERVER_ERROR, "Error during date parsing",
                    dateTimeParseException);
        }
    }

}
