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

import java.io.File;
import java.io.IOException;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.nio.file.Files;
import java.util.Set;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DirectoryPathUtils {
    private static final String FORWARDSLASH = "/";
    private static final String ALL_PERMISSIONS_STRING = "rwxrw-rw-";

    /**
     * If adjustable directory has absolute path, checks, that it begins with rootFolder and exists, creates if
     * needed.
     * If adjustable directory has relative path, adds rootFolder to its beginning, checks its existence and
     * creates, if needed.
     *
     * @param nestedFolder Directory, which should be checked for correction path
     * @param rootFolder Primary working directory from properties
     * @return Adjusted directory path with added primary directory added if needed
     */
    public static String buildProperDir(final String rootFolder, final String nestedFolder) {
        final StringBuilder properDir = new StringBuilder();
        final File nestedFolderPath = new File(nestedFolder);
        if (nestedFolderPath.isAbsolute()) {
            if (!nestedFolder.startsWith(rootFolder)) {
                final String errorMessage = "Nested folder path is absolute, but doesn't start with " +
                        "grid.engine.shared.folder";
                log.error(errorMessage);
                throw new IllegalStateException(errorMessage);
            }
            if (!nestedFolderPath.exists()) {
                try {
                    if (nestedFolderPath.mkdirs()) {
                        log.info("Directory with path " + nestedFolderPath + " was created.");
                        grantAllPermissionsToFolder(nestedFolderPath);
                    } else {
                        throw new IOException("Failed to create directory with path " + nestedFolderPath);
                    }
                } catch (final Exception exception) {
                    log.error(exception.getMessage());
                    exception.printStackTrace();
                }
            }
            properDir.append(nestedFolderPath);
        } else {
            if (rootFolder.endsWith(FORWARDSLASH)) {
                properDir.append(rootFolder).append(nestedFolderPath);
            } else {
                properDir.append(rootFolder).append(FORWARDSLASH).append(nestedFolderPath);
            }
            log.info("Nested folder path was changed to " + properDir);
            final File gridEnginePath = new File(properDir.toString());
            if (!gridEnginePath.exists()) {
                try {
                    if (gridEnginePath.mkdirs()) {
                        log.info(gridEnginePath + " Grid Shared Folder was created.");
                        grantAllPermissionsToFolder(gridEnginePath);
                    } else {
                        throw new IOException("Failed to create directory with path " + gridEnginePath);
                    }
                } catch (final Exception exception) {
                    log.error(exception.getMessage());
                    exception.printStackTrace();
                }
            }
        }
        return properDir.toString();
    }

    private static void grantAllPermissionsToFolder(final File directory) {
        try {
            final Set<PosixFilePermission> permissions = PosixFilePermissions.fromString(ALL_PERMISSIONS_STRING);
            Files.setPosixFilePermissions(directory.toPath(), permissions);
        } catch (final IOException exception) {
            log.error("Error while granting permissions to " + directory);
            exception.printStackTrace();
        }
    }
}
