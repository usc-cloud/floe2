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

package edu.usc.pgroup.floe.coordinator;

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.signals.SignalHandler;
import edu.usc.pgroup.floe.thriftgen.AppNotFoundException;
import edu.usc.pgroup.floe.thriftgen.AppStatus;
import edu.usc.pgroup.floe.thriftgen.ScaleDirection;
import edu.usc.pgroup.floe.thriftgen.TCoordinator;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.thriftgen.TSignal;
import edu.usc.pgroup.floe.utils.Utils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Channel;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Set;

/**
 * Implements the thrift's interface for the coordinator.
 *
 * @author Alok Kumbhare
 */
public class CoordinatorHandler implements TCoordinator.Iface {
    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CoordinatorHandler.class);

    /**
     * Internal property for storing open file handles.
     */
    private static final String FILE_HANDLES_PROP =
            "floe.internal.filehandlers";

    /**
     * Ping function.
     *
     * @param pingText text to reply.
     * @return returns the original pingText.
     * @throws TException thrift exception.
     */
    @Override
    public final String ping(final String pingText) throws TException {
        LOGGER.info("Received: '" + pingText + "' from client");
        return pingText;
    }

    /**
     * To start a chunck by chunck file upload.
     *
     * @param filename name of the file on server to be stored.
     * @return file identifier to be used in subsequent uploadChuck and
     * finishUpload functions.
     * @throws TException thrift exception wrapper.
     */
    @Override
    public final int beginFileUpload(final String filename) throws TException {
        if (!FloeConfig.getConfig().containsKey(FILE_HANDLES_PROP)) {
            FloeConfig.getConfig().setProperty(FILE_HANDLES_PROP,
                    new HashMap<Integer, Channel>());
        }

        HashMap<Integer, Channel> fidMap =
                (HashMap<Integer, Channel>) FloeConfig
                        .getConfig().getProperty(
                                FILE_HANDLES_PROP
                        );

        int fid = getNextAvailableFid();

        String uploadLocation = FloeConfig.getConfig().getString(
                ConfigProperties.FLOE_EXEC_SCRATCH_FOLDER)
                + Utils.Constants.FILE_PATH_SEPARATOR
                + FloeConfig.getConfig().getString(
                ConfigProperties.COORDINATOR_FILE_UPLOAD_FOLDER);

        try {
            //create folders if required.
            Path target = Paths.get(uploadLocation);
            Files.createDirectories(target);

            //File path
            String filePath = uploadLocation
                    + Utils.Constants.FILE_PATH_SEPARATOR
                    + filename;

            WritableByteChannel channel = Channels
                    .newChannel(new FileOutputStream(filePath));

            fidMap.put(fid, channel);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
            throw new TException(e);
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
            throw new TException(e);
        }

        LOGGER.info("Opened file:" + filename + " for upload. FID: " + fid);
        return fid;
    }

    /**
     * Upload chunk at a time.
     *
     * @param fid   File id returned earlier by beginFileUpload function.
     * @param chunk binary chunk
     * @throws TException thrift exception wrapper.
     */
    @Override
    public final void uploadChunk(final int fid, final ByteBuffer chunk)
            throws TException {
        if (!FloeConfig.getConfig().containsKey(FILE_HANDLES_PROP)) {
            throw new TException("File Handler Not found.");
        }

        HashMap<Integer, Channel> fidMap =
                (HashMap<Integer, Channel>) FloeConfig
                        .getConfig().getProperty(
                                FILE_HANDLES_PROP
                        );

        if (!fidMap.containsKey(fid)) {
            throw new TException("File Handler Not found.");
        }

        WritableByteChannel channel = (WritableByteChannel) fidMap.get(fid);
        try {
            synchronized (channel) {
                channel.write(chunk);
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
            throw new TException(e);
        }
        LOGGER.info("Uploaded chuck for FID: " + fid);
    }

    /**
     * Finish the file upload.
     *
     * @param fid File Identifier returned earlier by beginFileUpload function.
     * @throws TException thrift exception wrapper.
     */
    @Override
    public final void finishUpload(final int fid) throws TException {
        if (!FloeConfig.getConfig().containsKey(FILE_HANDLES_PROP)) {
            throw new TException("File Handler Not found.");
        }

        HashMap<Integer, Channel> fidMap =
                (HashMap<Integer, Channel>) FloeConfig
                        .getConfig().getProperty(
                                FILE_HANDLES_PROP
                        );

        if (!fidMap.containsKey(fid)) {
            throw new TException("File Handler Not found.");
        }

        WritableByteChannel channel = (WritableByteChannel) fidMap.get(fid);
        try {
            channel.close();
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
            throw new TException(e);
        }
        fidMap.remove(fid);
        LOGGER.info("Finished uploading file with FID: " + fid);
    }

    /**
     * To start a chunck by chunck file download.
     *
     * @param filename name of the file on server to downloaded.
     * @return file identifier to be used in subsequent uploadChuck and
     * finishUpload functions.
     * @throws TException thrift exception wrapper.
     */
    @Override
    public final synchronized int beginFileDownload(final String filename)
            throws TException {
        if (!FloeConfig.getConfig().containsKey(FILE_HANDLES_PROP)) {
            FloeConfig.getConfig().setProperty(FILE_HANDLES_PROP,
                    new HashMap<Integer, Channel>());
        }

        HashMap<Integer, Channel> fidMap =
                (HashMap<Integer, Channel>) FloeConfig
                        .getConfig().getProperty(
                                FILE_HANDLES_PROP
                        );

        int fid = getNextAvailableFid();

        String uploadLocation = FloeConfig.getConfig().getString(
                ConfigProperties.FLOE_EXEC_SCRATCH_FOLDER)
                + Utils.Constants.FILE_PATH_SEPARATOR
                + FloeConfig.getConfig().getString(
                ConfigProperties.COORDINATOR_FILE_UPLOAD_FOLDER);

        try {
            //create folders if required.
            Path target = Paths.get(uploadLocation);
            Files.createDirectories(target);

            //File path
            String filePath = uploadLocation
                    + Utils.Constants.FILE_PATH_SEPARATOR
                    + filename;

            ReadableByteChannel channel = Channels
                    .newChannel(new FileInputStream(filePath));

            fidMap.put(fid, channel);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
            throw new TException(e);
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
            throw new TException(e);
        }

        LOGGER.info("Opened file:" + filename + " for download. FID: " + fid);
        return fid;
    }

    /**
     * Returns one chunk of a file at a time (default size=4KB).
     *
     * @param fid File Identifier returned by beginFileDownloads function.
     * @return ByteBuffer (client should check size == 0) to determine if
     * download has finished.
     * @throws TException thrift exception wrapper.
     */
    @Override
    public final ByteBuffer downloadChunk(final int fid) throws TException {
        if (!FloeConfig.getConfig().containsKey(FILE_HANDLES_PROP)) {
            throw new TException("File Handler Not found.");
        }

        HashMap<Integer, Channel> fidMap =
                (HashMap<Integer, Channel>) FloeConfig
                        .getConfig().getProperty(
                                FILE_HANDLES_PROP
                        );

        if (!fidMap.containsKey(fid)) {
            throw new TException("File Handler Not found.");
        }

        ReadableByteChannel channel = (ReadableByteChannel) fidMap.get(fid);
        ByteBuffer buffer = ByteBuffer.allocate(
                Utils.Constants.BUFFER_SIZE);
        try {
            synchronized (channel) {
                if (channel.read(buffer) <= 0) {
                    //close channel and remove fid.

                    channel.close();
                    fidMap.remove(fid);
                }
                buffer.flip();
            }
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error(e.getMessage());
            throw new TException(e);
        }
        LOGGER.info("sent chuck buffer limit: " + buffer.limit());
        return buffer;
    }

    /**
     * Service call to submit a floe app.
     * @param appName name of the app
     * @param app The app topology.
     * @throws TException thrift exception wrapper.
     */
    @Override
    public final void submitApp(final String appName, final TFloeApp app)
            throws TException {
        Coordinator.getInstance().submitApp(appName, app);
    }

    /**
     * Service call to submit a floe app.
     * @param appName name of the app
     * @throws TException thrift exception wrapper.
     */
    @Override
    public final void killApp(final String appName) throws TException {
        Coordinator.getInstance().killApp(appName);
    }

    /**
     * Service call to handle the scale event at runtime.
     * @param direction direction of scaling
     * @param appName name of the app
     * @param pelletName name of the pellet
     * @param count number of instances to be scaled up or down.
     * throws InsufficientResourcesException if enough containers are not
     * available.
     * throws AppNotFoundException if the given appName does is not running.
     * throws PelletNotFoundException if the application does not contain a
     * pellet with the given name.
     * @throws TException Any other exceptions wrapped into TException.
     */
    @Override
    public final void scale(final ScaleDirection direction,
                            final String appName,
                            final String pelletName, final int count)
            throws
            //InsufficientResourcesException,
            //AppNotFoundException,
            //PelletNotFoundException,
            TException {

        Coordinator.getInstance().scale(
                direction,
                appName,
                pelletName,
                count
        );
    }

    /**
     * Service call to handle the signal event at runtime.
     * @param signal the TSignal object sent by the client.
     * @throws TException Thrift exception wrapper.
     */
    @Override
    public final void signal(final TSignal signal)
            throws
            //AppNotFoundException,
            //PelletNotFoundException,
            TException {

        SignalHandler.getInstance().signal(
                signal.get_destApp(),
                signal.get_destPellet(),
                signal.get_data()
        );
    }

    /**
     * Service call to handle switch alternate.
     * @param appName name of the app.
     * @param pelletName name of the pellet.
     * @param alternateName alternate to switch to.
     * @throws TException TException Thrift exception wrapper.
     */
    @Override
    public final void switchAlternate(final String appName,
                                      final String pelletName,
                                      final String alternateName)
            throws
            //AppNotFoundException,
            //PelletNotFoundException,
            //AlternateNotFoundException,
            TException {

        Coordinator.getInstance().switchAlternate(
            appName,
            pelletName,
            alternateName
        );
    }

    /**
     * Service call to get the status of an application.
     * @param appName application name to check the status.
     * @return the current status.
     * //throws AppNotFoundException if the application is not found
     * @throws TException TException Thrift exception wrapper.
     */
    @Override
    public final AppStatus getAppStatus(final String appName)
            throws
            //AppNotFoundException,
            TException {

        try {
            AppStatus status
                    = Coordinator.getInstance().getApplicationStatus(appName);
            if (status == null) {
                throw new AppNotFoundException("Application " + appName
                        + "does not exist or has been terminated.");
            }
            return status;
        } catch (Exception e) {
            LOGGER.error("Could not retrieve app status.");
            throw new TException(e);
        }
    }

    /**
     * Returns an available fid.
     * Note: this is a synchronized function so that multiple accesses are
     * handled correctly.
     *
     * @return a valid fileid to be used in subsequent file operations.
     */
    private synchronized int getNextAvailableFid() {
        int fid;

        if (!FloeConfig.getConfig().containsKey(FILE_HANDLES_PROP)) {
            FloeConfig.getConfig().setProperty(FILE_HANDLES_PROP,
                    new HashMap<Integer, Channel>());
        }

        HashMap<Integer, Channel> fidMap =
                (HashMap<Integer, Channel>) FloeConfig
                        .getConfig().getProperty(
                                FILE_HANDLES_PROP
                        );

        Set<Integer> fids = fidMap.keySet();
        //FIXME: is this ok? can we do something faster.

        fid = 0;
        for (int lf : fids) {
            if (lf > fid) {
                fid = lf;
            }
        }

        fid++;
        return fid;
    }
}
