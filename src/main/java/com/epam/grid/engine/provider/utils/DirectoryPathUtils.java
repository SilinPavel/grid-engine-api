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

package com.epam.grid.engine.provider.utils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.Files;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DirectoryPathUtils {
    private static final String ALL_PERMISSIONS_STRING = "rwxrw-rw-";

    /**
     * If nestedFolder directory has absolute path, checks, that it begins with rootFolder and exists, creates if
     * needed.
     * If nestedFolder directory has relative path, adds rootFolder to its beginning, checks its existence and
     * creates, if needed.
     *
     * @param nestedFolder Directory, which should be checked for correction path
     * @param rootFolder   Primary working directory from properties
     * @return Adjusted directory path with added primary directory added if needed
     */
    @SuppressWarnings("PMD.PreserveStackTrace")
    public static Path buildProperDir(final String rootFolder, final String nestedFolder) {
        Path processingPath = Path.of(nestedFolder);
        if (processingPath.isAbsolute()) {
            if (!nestedFolder.startsWith(rootFolder)) {
                throw new IllegalStateException("Nested folder path is absolute, but doesn't start with "
                        + "grid.engine.shared.folder");
            }
        } else {
            processingPath = Paths.get(rootFolder, nestedFolder);
            log.info("Nested folder path was changed to " + processingPath);
        }
        checkIfFolderNotExistsAndCreate(processingPath);
        return processingPath;
    }

    private static void checkIfFolderNotExistsAndCreate(final Path folderToCreate) {
        if (!Files.exists(folderToCreate)) {
            try {
                if (folderToCreate.toFile().mkdirs()) {
                    log.info("Directory with path " + folderToCreate + " was created.");
                    grantAllPermissionsToFolder(folderToCreate);
                } else {
                    throw new IllegalArgumentException("Failed to create directory with path " + folderToCreate);
                }
            } catch (final Exception exception) {
                throw new IllegalArgumentException("Failed to create a directory by provided path: "
                        + folderToCreate + ". " + exception.getMessage(), exception);
            }
        }
    }

    private static void grantAllPermissionsToFolder(final Path directory) throws IOException {
        final Set<PosixFilePermission> permissions = PosixFilePermissions.fromString(ALL_PERMISSIONS_STRING);
        Files.setPosixFilePermissions(directory, permissions);
    }
}
