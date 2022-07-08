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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;

/**
 * This class performs the formation of the structure of the executed command
 * according to the template.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class CommandArgUtils {

    private static final char CR = '\r';
    private static final char LF = '\n';
    private static final char TAB = '\t';
    private static final char SPACE = ' ';
    private static final char QUOTE = '"';
    private static final char BACKSLASH = '\\';

    /**
     * This method forms a command structure from a string command.
     *
     * @param command The command from the template engine.
     * @return The command's structure that is ready for execution.
     */
    public static String[] splitCommandIntoArgs(final String command) {
        final List<String> result = new ArrayList<>();
        final StringBuilder token = new StringBuilder();

        boolean isQuote = false;
        boolean isToken = false;
        boolean isQuoteEscapedToken = false;

        for (int i = 0; i < command.length(); i++) {
            if (!isToken) {
                if (isWhitespaceCharacter(command.charAt(i))) {
                    continue;
                }
                isToken = true;
            }

            if (!isQuote && isWhitespaceCharacter(command.charAt(i))) {
                result.add(token.toString());
                token.setLength(0);
                isToken = false;
                continue;
            }

            if (command.charAt(i) == QUOTE) {
                if (token.length() == 1 && command.charAt(i - 1) == BACKSLASH) { //a start of token with escaped quote
                    isQuoteEscapedToken = true;
                    isQuote = true;
                } else {
                    if (!isQuote || ((i - 1 > 0) && command.charAt(i - 1) != BACKSLASH)) {
                        isQuote = !isQuote;
                    }
                    if (isQuoteEscapedToken) {
                        isQuote = false;
                    }
                    isQuoteEscapedToken = false;
                }
            }
            token.append(command.charAt(i));
        }

        if (token.length() != 0) {
            result.add(token.toString());
        }
        return result.toArray(new String[0]);
    }

    private static boolean isWhitespaceCharacter(final char ch) {
        switch (ch) {
            case CR:
            case LF:
            case SPACE:
            case TAB:
                return true;
        }
        return false;
    }
}
