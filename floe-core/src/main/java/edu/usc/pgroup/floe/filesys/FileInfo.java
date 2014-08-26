/*
 * Copyright 2014 University of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.usc.pgroup.floe.filesys;

import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * The FileInfo object used for directory monitoring.
 * @author kumbhare
 */
public class FileInfo {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FileInfo.class);

    /**
     * File name for which this file info is created.
     */
    private final String fileName;

    /**
     * File contents as byte array.
     */
    private byte[] contents;

    /**
     * Constructor.
     * @param file (Absolute) File name for which this file info is created.
     */
    public FileInfo(final String file) {
        this.fileName = file;
        this.contents = null;
    }

    /**
     * @return Gets the file name
     */
    public final String getFileName() {
        return fileName;
    }

    /**
     * Read the entire contents of the file.
     */
    public final void readFileContents() {
        try {
            contents = FileUtils.readFileToByteArray(new File(fileName));
        } catch (IOException e) {
            LOGGER.warn("Could not open file for read. Exception: {}", e);
        }
    }

    /**
     * Gets the file contents.
     * @return File contents as byte array. Returns null if the file hasn't
     * been read yet.
     */
    public final byte[] getContents() {
        return contents;
    }
}
