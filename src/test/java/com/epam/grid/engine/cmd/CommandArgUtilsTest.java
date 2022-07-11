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

package com.epam.grid.engine.cmd;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.Map;
import java.util.stream.Stream;

class CommandArgUtilsTest {

    private static final String EMPTY_STRING = "";
    private static final String ONE_VARIABLE_NAME = "oneVariable";
    private static final String ONE_VALUE_WITH_SPACES = "a value with spaces";

    @ParameterizedTest
    @MethodSource("provideCasesToParseCommand")
    public void shouldSplitCommandIntoTokenArgs(final String commandString, final String[] expectedCommand) {
        Assertions.assertArrayEquals(expectedCommand, CommandArgUtils.splitCommandIntoArgs(commandString));
    }

    static Stream<Arguments> provideCasesToParseCommand() {
        return Stream.of(
                Arguments.of("qhost -h current_host1 current_host2 current_host3 -xml",
                        new String[]{"qhost", "-h", "current_host1", "current_host2", "current_host3", "-xml"}),

                Arguments.of("qhost\n-h\n \n current_host\n \n\n-xml\n",
                        new String[]{"qhost", "-h", "current_host", "-xml"}),

                Arguments.of("qhost\r\n\r\n-h\r\n \r\n current_host\r\n \r\n\r\n-xml\r\n",
                        new String[]{"qhost", "-h", "current_host", "-xml"}),

                Arguments.of("\\\"some token is entirely enclosed in escaped quotes\\\"",
                        new String[]{"\\\"some token is entirely enclosed in escaped quotes\\\""}),

                Arguments.of("\"test \\\"escape\tquoted text\\\"\"\t\r\n some\t text",
                        new String[]{"\"test \\\"escape\tquoted text\\\"\"", "some", "text"}),

                Arguments.of("sbatch\n\n    --export \"ALL,additionalProp1=value1,additionalProp3=value2\"\n\n\n\n\n\n"
                                + "    --job-name=someTaskName\n\n\n    --partition=someQueue\n\n\n    --chdir=/data\n"
                                + "\n\n    \n    --some=5 --comment=\"some commentary in a few words  \\\\\\\" and a f"
                                + "ew more words\"\n    \n\n/data/test.py\n",
                        new String[]{"sbatch", "--export", "\"ALL,additionalProp1=value1,additionalProp3=value2\"",
                                     "--job-name=someTaskName", "--partition=someQueue", "--chdir=/data", "--some=5",
                                     "--comment=\"some commentary in a few words  \\\\\\\" and a few more words\"",
                                     "/data/test.py"})
        );
    }

    @ParameterizedTest
    @MethodSource("provideEnvironmentVariablesCases")
    public void shouldReturnEnvironmentVariablesAsCommandArgument(final String expectedArgument,
                                                                  final Map<String, String> variables) {
        Assertions.assertEquals(expectedArgument, CommandArgUtils.envVariablesMapToString(variables));
    }

    static Stream<Arguments> provideEnvironmentVariablesCases() {
        return Stream.of(
                Arguments.of(EMPTY_STRING, Map.of(EMPTY_STRING, EMPTY_STRING)),
                Arguments.of(ONE_VARIABLE_NAME, Map.of(ONE_VARIABLE_NAME, EMPTY_STRING)),
                Arguments.of(String.format("%s=%s", ONE_VARIABLE_NAME, ONE_VALUE_WITH_SPACES),
                        Map.of(ONE_VARIABLE_NAME, ONE_VALUE_WITH_SPACES))
        );
    }
}
