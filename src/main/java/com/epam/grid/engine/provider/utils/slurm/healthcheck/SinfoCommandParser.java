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
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.epam.grid.engine.utils.TextConstants.SPACE;
import static java.util.stream.Collectors.joining;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SinfoCommandParser {

    private static final String STATUS = "AVAIL";
    private static final String CANT_FIND_CONNECTION = "can't find connection";
    private static final String CANT_FIND_CONNECTION_MESSAGE = "Can`t find connection via specified port";
    private static final String DATE_FORMAT_CHECK_TIME = "MM/dd/yyyy HH:mm:ss:";
    private static final String DATE_FORMAT_START_TIME = "MM/dd/yyyy HH:mm:ss";
    private static final Long NOT_PROVIDED = 99999L;

    public static HealthCheckInfo parseSinfoResult(final CommandResult commandResult) {
        validateSinfoResponse(commandResult);
        final Map<String, String> stdOut = getResponse(commandResult.getStdOut());
        final StatusInfo statusInfo = parseStatusInfo(stdOut);
        return HealthCheckInfo.builder()
                .statusInfo(statusInfo)
                .checkTime(parseCheckTime(stdOut))
                .startTime(parseStartTime(stdOut))
                .build();
    }

    private static void validateSinfoResponse(final CommandResult commandResult) {
        if (CollectionUtils.isEmpty(commandResult.getStdOut()) || commandResult.getStdOut().size() < 2) {
            throw new GridEngineException(HttpStatus.NOT_FOUND,
                    String.format("SLURM error during health check. %nexitCode = %d %nstdOut: %s %nstdErr: %s",
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

    private static StatusInfo parseStatusInfo(final Map<String, String> stdOut) {
        final long statusCode = parseStatusCode(stdOut);
        return StatusInfo.builder()
                .code(statusCode)
                .status(parseStatus(statusCode))
                .info(parseInfo(stdOut))
                .build();
    }

    private static long parseStatusCode(final Map<String, String> stdOut) {
        final String status=stdOut.get(STATUS);
        long statusCode = NOT_PROVIDED;
        switch (status) {
            case "up":
                statusCode = 0L;
                break;
            case "down":
                statusCode = 2L;
                break;
        }
        return statusCode;
    }

    private static GridEngineStatus parseStatus(final long status) {
        return GridEngineStatus.getById(status)
                .orElseThrow(() -> new GridEngineException(HttpStatus.INTERNAL_SERVER_ERROR,
                        "Not valid status value provided: " + status));
    }

    private static String parseInfo(final Map<String, String> stdOut) {
        return stdOut.entrySet()
                .stream()
                .map(Object::toString)
                .collect(joining(" "));
    }

    private static LocalDateTime parseCheckTime(final Map<String, String> stdOut) {
        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern(DATE_FORMAT_CHECK_TIME);
        return tryParseStringToLocalDateTime(new SimpleDateFormat(DATE_FORMAT_CHECK_TIME).format(new Date()),
                formatter);
    }

    private static LocalDateTime parseStartTime(final Map<String, String> stdOut) {
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

    private static Map<String, String> getResponse(final List<String> response) {
        Map<String, String> mapResponse = null;
        if (response.size() > 1) {
            List<String> keys = Arrays.stream(response.get(0).replaceAll("\\\\s+", " ").split(SPACE))
                    .filter(s->s.length()>0)
                    .collect(Collectors.toList());
            List<String> values = Arrays.stream(response.get(1).replaceAll("\\\\s+", " ").split(SPACE))
                    .filter(s->s.length()>0)
                    .collect(Collectors.toList());
            mapResponse = IntStream.range(0, keys.size())
                    .boxed()
                    .collect(Collectors.toMap(keys::get, values::get));
        }
        else{
            throw new GridEngineException(HttpStatus.INTERNAL_SERVER_ERROR, "Server response is empty or incomplete");
        }
        return mapResponse;
    }

}
