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

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.DirectoryIteratorException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Cache and listener for directory and its contents.
 * @author kumbhareMap
 */
public class DirectoryCache {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(DirectoryCache.class);

    /**
     * The path of the directory being watched.
     */
    private final Path watchedDirectory;

    /**
     * Flag to indicate whether or not read and cache file contents as well.
     * This is fine for small files, since it will read the entire file
     * whenever there is an update to the file. Not recommended for
     * potentially large files.
     */
    private final boolean isCacheFileContents;


    /**
     * Cached data map (from base file name to FileInfo object).
     */
    private Map<String, FileInfo> cachedData;


    /**
     * List of update listeners.
     * Will be notified when any of the directory events such as CreateFile,
     * RemoveFile, UpdateFile happens.
     */
    private List<DirectoryUpdateListener> updateListeners;

    /**
     * The monitor thread.
     */
    private Thread monitorThread;

    /**
     * Contructor.
     * @param directory the directory to cache and watch.
     * @param cacheFileContents if true, the contents of the files are also
     *                          cached.
     * TODO: Recursive watch is not supported yet.
     */
    public DirectoryCache(final Path directory,
                          final boolean cacheFileContents) {
        this.watchedDirectory = directory;
        this.isCacheFileContents = cacheFileContents;
        this.updateListeners = new ArrayList<DirectoryUpdateListener>();
        this.cachedData = new HashMap<String, FileInfo>();
    }

    /**
     * Get the current cached data.
     * @return A collection of cached data items.
     */
    public final Collection<FileInfo> getCurrentCachedData() {
        return cachedData.values();
    }

    /**
     * Gets the FilInfo cached data associated with the given file.
     * @param fileName Name of a file in the monitored directory.
     * @return FileInfo object containing metadata and data (if
     * cacheFileContents = true)
     */
    public final FileInfo getCurrentCachedData(final String fileName) {
        return cachedData.get(fileName);
    }

    /**
     * Adds a directory update listener.
     * @param listener Implementation of the DirectoryUpdateListener Interface.
     */
    public final void addListener(final DirectoryUpdateListener listener) {
        updateListeners.add(listener);
    }


    /**
     * Start monitoring the directory.
     * @throws java.io.IOException Throws an IOException if the directory
     * path is not found.
     */
    public final void start() throws IOException {
        if (monitorThread == null) {
            monitorThread = new Thread(
                    new DirectoryCacheMonitor(watchedDirectory));
        }

        if (!monitorThread.isAlive()) {
            monitorThread.start();
        }
    }

    /**
     * Stop monitoring the directory.
     */
    public final void stop() {
        if (monitorThread != null && monitorThread.isAlive()) {
            monitorThread.interrupt();
        }
    }

    /**
     * Internal class to run the directory monitoring thread.
     */
    class DirectoryCacheMonitor implements Runnable {

        /**
         * Directory watcher service.
         */
        private final WatchService watcher;

        /**
         * The watch key corresponding to the watched directory.
         */
        private WatchKey watchKey;

        /**
         * Constructor.
         * @param dir Directory path being watched.
         * @throws IOException Throws an IOException if the directory
         * path is not found.
         */
        DirectoryCacheMonitor(final Path dir) throws IOException {
            this.watcher = dir.getFileSystem().newWatchService();
            LOGGER.info("Monitoring Directory: " + dir.toString());

            watchKey = dir.register(watcher,
                    StandardWatchEventKinds.ENTRY_CREATE,
                    StandardWatchEventKinds.ENTRY_DELETE,
                    StandardWatchEventKinds.ENTRY_MODIFY);
        }

        /**
         * The Monitor thread's run method.
         * This will execute the event processing loop.
         */
        @Override
        public void run() {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(
                    watchedDirectory)) {
                for (Path entry: stream) {
                    String absoluteName = watchedDirectory
                            .toAbsolutePath().toString()
                            + FloeConfig.getConfig().getString(
                            ConfigProperties.SYS_FILE_SEPARATOR)
                            + entry.getFileName();

                    FileInfo fileInfo = new FileInfo(
                            absoluteName);

                    if (isCacheFileContents) {
                        fileInfo.readFileContents();
                    }

                    cachedData.put(fileInfo.getFileName(), fileInfo);
                    LOGGER.info("File added to cachce: " + absoluteName);
                }

                for (DirectoryUpdateListener listener
                        : updateListeners) {
                    listener.childrenListInitialized(cachedData.values());
                }
            } catch (DirectoryIteratorException e) {
                // I/O error encountered during the iteration,
                // the cause is an IOException
                LOGGER.error("Error occurred while monitoring directory. "
                        + "No longer monitoring the directory. "
                        + "Exception: {}", e);
                return;
            } catch (IOException e) {
                LOGGER.error("Error occurred while monitoring directory. "
                        + "No longer monitoring the directory. "
                        + "Exception: {}", e);
                return;
            }

            for (;;) {
                try {
                    watchKey = watcher.take();
                } catch (InterruptedException e) {
                    LOGGER.error("Error occurred while monitoring directory. "
                                    + "No longer monitoring the directory. "
                                    + "Exception: {}", e);

                    return;
                }

                for (WatchEvent<?> watchEvent: watchKey.pollEvents()) {
                    WatchEvent.Kind<?> kind = watchEvent.kind();
                    if (StandardWatchEventKinds.ENTRY_CREATE == kind) {


                        //Add info to the map.
                        Path addedPath = ((WatchEvent<Path>) watchEvent)
                                .context();

                        String absoluteName = watchedDirectory
                                .toAbsolutePath().toString()
                                + FloeConfig.getConfig().getString(
                                    ConfigProperties.SYS_FILE_SEPARATOR)
                                + addedPath.toString();

                        FileInfo fileInfo = new FileInfo(
                                absoluteName);

                        if (isCacheFileContents) {
                            fileInfo.readFileContents();
                        }

                        cachedData.put(fileInfo.getFileName(), fileInfo);

                        for (DirectoryUpdateListener listener
                                : updateListeners) {
                            listener.childAdded(fileInfo);
                        }

                        LOGGER.info("File added to cachce: " + absoluteName);
                    } else if (StandardWatchEventKinds.ENTRY_DELETE == kind) {
                        //Remove info from the map.
                        Path removedPath = ((WatchEvent<Path>) watchEvent)
                                .context();

                        String absoluteName = watchedDirectory
                                .toAbsolutePath().toString()
                                + FloeConfig.getConfig().getString(
                                    ConfigProperties.SYS_FILE_SEPARATOR)
                                + removedPath.toString();

                        FileInfo fileInfo = null;
                        if (cachedData.containsKey(absoluteName)) {
                            fileInfo = cachedData.remove(absoluteName);
                        } else {
                            LOGGER.warn("The file being removed is not "
                                    + "recognized.");
                        }

                        for (DirectoryUpdateListener listener
                                : updateListeners) {
                            listener.childRemoved(fileInfo);
                        }
                        LOGGER.info("File removed from cache:" + absoluteName);

                    } else if (StandardWatchEventKinds.ENTRY_MODIFY == kind) {
                        //Update the info.
                        //Remove info from the map.
                        Path updatedPath = ((WatchEvent<Path>) watchEvent)
                                .context();

                        String absoluteName = watchedDirectory
                                .toAbsolutePath().toString()
                                + FloeConfig.getConfig().getString(
                                    ConfigProperties.SYS_FILE_SEPARATOR)
                                + updatedPath.toString();

                        FileInfo fileInfo = null;
                        if (cachedData.containsKey(absoluteName)) {
                            fileInfo = cachedData.get(absoluteName);
                        } else {
                            LOGGER.warn("The file being removed is not "
                                    + "recognized.");
                        }


                        if (fileInfo != null) {
                            //update the info here.

                            if (isCacheFileContents) {
                                fileInfo.readFileContents();
                            }
                        }
                        for (DirectoryUpdateListener listener
                                : updateListeners) {
                            listener.childUpdated(fileInfo);
                        }
                        LOGGER.info("File updated in cache:" + absoluteName);
                    }


                }

                if (!watchKey.reset()) {
                    LOGGER.error("Could not reset the directory watch. No "
                            + "longer monitoring the directory.");
                    return;
                }
            }
        }
    }
}
