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

package edu.usc.pgroup.floe.client;

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.thriftgen.TCoordinator;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.utils.Utils;
import org.apache.commons.io.FilenameUtils;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.File;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Client API for FLOE.
 *
 * @author Alok Kumbhare
 */
public class FloeClient extends ThriftClient {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FloeClient.class);

    /**
     * Single floe client instance.
     */
    private static FloeClient instance;

    /**
     * the client object to communicate with the Coordinator.
     */
    private final TCoordinator.Client client;

    /**
     * default public constructor. Establishes connection with the Floe Server.
     *
     * @throws TTransportException Exception.
     */
    public FloeClient() throws TTransportException {
        super();
        if (getProtocol() != null) {
            client = new TCoordinator.Client(getProtocol());
        } else {
            throw new RuntimeException(
                 "Error occurred while establishing connection to the server.");
        }
    }


    /**
     * client entry point.
     *
     * @param args command line arguments.
     */
    public static void main(final String[] args) {
        try {

            LOGGER.info(FloeConfig.getConfig().getString(
                    ConfigProperties.CONTAINER_HEARTBEAT_PERIOD));

            FloeClient client = new FloeClient();
            String reply = client.getClient().ping("Ping Text");
            LOGGER.info("Received reply: '" + reply + "' from the server.");

            LOGGER.info("Uploading file:" + args[0]);
            client.uploadFileSync(args[0]);
            LOGGER.info("Finished uploading file:" + args[0]);

            LOGGER.info("Downloading file:" + args[0]);
            client.downloadFileSync(args[0], "downloaded.jar");
            LOGGER.info("Finished Downloading file:" + args[0]);
        } catch (TTransportException e) {
            e.printStackTrace();
            LOGGER.error("Exception occurred while connecting");
            LOGGER.error(e.getMessage());
        } catch (TException e) {
            e.printStackTrace();
            LOGGER.error("Exception occurred while pinging");
            LOGGER.error(e.getMessage());
        }
    }

    /**
     * get the thrift client stub object.
     *
     * @return configured and connected thrift client.
     */
    public final TCoordinator.Client getClient() {
        return client;
    }

    /**
     * Uploads the file to the coordinator.
     * The file is uploaded relative to the coordinator's scratch folder.
     *
     * @param fileName name of the file to be stored on the coordinator.
     * @return the base fileName which may be used for downloading the file
     * later.
     */
    public final String uploadFileSync(final String fileName) {
        String baseFile = FilenameUtils.getName(fileName);
        try {
            int fid = getClient().beginFileUpload(baseFile);
            ReadableByteChannel inChannel =
                    Channels.newChannel(new FileInputStream(fileName));
            ByteBuffer buffer = ByteBuffer.allocate(
                    Utils.Constants.BUFFER_SIZE);
            while (inChannel.read(buffer) > 0) {
                buffer.flip();
                getClient().uploadChunk(fid, buffer);
                buffer.clear();
            }
            inChannel.close();
            getClient().finishUpload(fid);
        } catch (TException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        }
        return baseFile;
    }

    /**
     * Download the file from the Coordinator's scratch folder.
     * Note: Filename should not be an absolute path or contain ".."
     *
     * @param fileName    name of the file to be downloaded from coordinator's
     *                    scratch.
     * @param outFileName (local) output file name.
     */
    public final void downloadFileSync(
            final String fileName, final String outFileName) {
        if (!Utils.checkValidFileName(fileName)) {
            throw new IllegalArgumentException("Filename is valid. Should not"
                    + " contain .. and should not be an absolute path.");
        }

        int rfid = 0;
        WritableByteChannel outChannel = null;
        try {
            rfid = getClient().beginFileDownload(fileName);
            File outFile = new File(outFileName);
            File parent = outFile.getParentFile();
            if (!parent.exists()) {
                parent.mkdirs();
            }
            outChannel = Channels.newChannel(new FileOutputStream(outFileName));
        } catch (TException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        } catch (FileNotFoundException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        }

        int written;
        try {
            do {
                ByteBuffer buffer = null;

                buffer = getClient().downloadChunk(rfid);

                written = outChannel.write(buffer);
                LOGGER.info(String.valueOf(written));
            } while (written != 0);
        } catch (TException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        } catch (IOException e) {
            LOGGER.error(e.getMessage());
            throw new RuntimeException(e);
        } finally {
            if (outChannel != null) {
                try {
                    outChannel.close();
                } catch (IOException e) {
                    LOGGER.error(e.getMessage());
                    throw new RuntimeException(e);
                }
            }
        }
    }

    /**
     * Download the file from the Coordinator's scratch folder.
     * Note: Filename should not be an absolute path or contain ".."
     *
     * @param fileName name of the file to be downloaded from the coordinator.
     */
    public final void downloadFileSync(final String fileName) {
        downloadFileSync(fileName, fileName);
    }

    /**
     * @return singleton FloeClient instance.
     * @throws TTransportException thrift exception.
     */
    public static synchronized FloeClient getInstance()
            throws TTransportException {
        if (instance == null) {
            instance = new FloeClient();
        }
        return instance;
    }

    /**
     * Submit the floe app to the coordinator.
     * @param appName name of the app.
     * @param app the FloeApp topology.
     * @throws TException thrift exception.
     */
    public final void submitApp(final String appName, final TFloeApp app) throws
            TException {
        getClient().submitApp(appName, app);
    }
}
