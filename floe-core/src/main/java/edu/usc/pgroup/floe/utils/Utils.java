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

package edu.usc.pgroup.floe.utils;

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.ObjectInputStream;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.file.FileSystems;
import java.nio.file.Path;

/**
 * Common Utility Class.
 *
 * @author Alok Kumbhare
 */
public final class Utils {


    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(Utils.class);

    /**
     * Hiding public constructor.
     */
    private Utils() {

    }

    /**
     * returns the canonical host name. This assumes a unique host name for
     * each machine in the cluster does not apply.
     * FixMe: In local cloud environment (local eucalyptus in system mode)
     * where the DNS server is not running, this might be an issue.
     *
     * @return the canonical hostname.
     */
    public static String getHostName() {
        try {
            return InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            LOGGER.error("Error occurred while retrieving hostname"
                    + e.getMessage());
            throw new RuntimeException("Error occurred while "
                    + "retrieving hostname" + e.getMessage());
        }
    }

    /**
     * Checks if the filename (to be uploaded or downloaded) is valid.
     * It should not contain ".." and should not be an absolute path.
     *
     * @param fileName filename to be uploaded or downloaded relative to the
     *                 coordinator's scratch folder.
     * @return returns true if the filename is valid and can be used in the
     * download or upload functions.
     */
    public static boolean checkValidFileName(final String fileName) {
        if (fileName.contains("..")) {
            return false;
        }

        Path filePath = FileSystems.getDefault().getPath(fileName);
        if (filePath.isAbsolute()) {
            return true;
        }
        return true;
    }

    /**
     * Returns a complete command for launching a java child process.
     * with SAME classpath as the parent.
     * @param className Child's class name.
     * @param jvmParams Additional params to be passed to the JVM
     * @param appParams custom application params, appended into a single
     *                  string.
     * @return command string to launch the given class in a JVM
     */
    public static String getJavaProcessLaunchCommand(final String className,
                                             final String appParams,
                                             final String... jvmParams) {
        String javaHome = FloeConfig.getConfig().getString(
                ConfigProperties.SYS_JAVA_HOME);

        String classPath = FloeConfig.getConfig().getString(
                ConfigProperties.SYS_JAVA_CLASS_PATH);

        //TODO: Add other required parameters

        String fileSeparator = FloeConfig.getConfig().getString(
                ConfigProperties.SYS_FILE_SEPARATOR);
        String javaCmd = javaHome + fileSeparator + "bin" + fileSeparator
                + "java";

        String params = StringUtils.join(jvmParams, ' ');

        String command = StringUtils.join(new String[]{javaCmd,
                "-cp " + classPath, params, className, appParams}, ' ');

        return command;
    }

    /**
     * Use reflection to create an instance of the given class.
     * @param fqdnClassName the fully qualified class name.
     * @return a new instance of the given class. NULL if there was an error
     * creating the instance.
     */
    public static Object instantiateObject(final String fqdnClassName) {
        Object instance = null;
        try {
            Class<?> klass = Class.forName(fqdnClassName);
            instance = klass.newInstance();
        } catch (ClassNotFoundException e) {
            LOGGER.error("Could not create an instance of {}, Exception:{}",
                    fqdnClassName, e);
        } catch (InstantiationException e) {
            LOGGER.error("Could not create an instance of {}, Exception:{}",
                    fqdnClassName, e);
        } catch (IllegalAccessException e) {
            LOGGER.error("Could not create an instance of {}, Exception:{}",
                    fqdnClassName, e);
        }
        return instance;
    }


    /**
     * Serializer used for storing data into zookeeper. We use the default
     * java serializer (since the amount of data to be serialized is usually
     * very small).
     * NOTE: THIS IS DIFFERENT FROM THE SERIALIZER USED DURING COMMUNICATION
     * BETWEEN DIFFERENT FLAKES. THAT ONE IS PLUGABBLE, THIS IS NOT.
     *
     * @param obj Object to be serialized
     * @return serialized byte array.
     */
    public static byte[] serialize(final Object obj) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(obj);
            oos.close();
            return bos.toByteArray();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    /**
     * Deserializer for Zookeeper data. See comments for serialize function.
     *
     * @param serialized serialized byte array (default java serialized)
     *                   obtained from the serialize function.
     * @return the constructed java object. Needs to be typecasted to
     * appropriate object before using. No checks are actionCompleted here.
     */
    public static Object deserialize(final byte[] serialized) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
            ObjectInputStream ois = new ObjectInputStream(bis);
            Object ret = ois.readObject();
            ois.close();
            return ret;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Deserializer for Zookeeper data. See comments for serialize function.
     *
     * @param serialized serialized byte array (default java serialized)
     *                   obtained from the serialize function.
     * @param classLoader Custom Class loader to used to deserialize the
     *                    object.
     * @return the constructed java object. Needs to be typecasted to
     * appropriate object before using. No checks are actionCompleted here.
     */
    public static Object deserialize(final byte[] serialized,
                                     final ClassLoader classLoader) {
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
            //ObjectInputStream ois = new ObjectInputStream(bis);
            ClassLoaderObjectInputStream ois
                    = new ClassLoaderObjectInputStream(classLoader, bis);
            Object ret = ois.readObject();
            ois.close();
            return ret;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * returns the pull local path of the application jar.
     * @param appName name of the application.
     * @param jarName jar name.
     * @return the path to the application jar.
     */
    public static String getContainerJarDownloadPath(final String appName,
                                                     final String jarName) {
        String downloadLocation = FloeConfig.getConfig().getString(
                ConfigProperties.FLOE_EXEC_SCRATCH_FOLDER)
                + Utils.Constants.FILE_PATH_SEPARATOR
                + FloeConfig.getConfig().getString(
                ConfigProperties.CONTAINER_LOCAL_FOLDER)
                + Utils.Constants.FILE_PATH_SEPARATOR
                + Utils.Constants.CONTAINER_APP_FOLDER
                + Utils.Constants.FILE_PATH_SEPARATOR
                + appName
                + Utils.Constants.FILE_PATH_SEPARATOR
                + jarName;

        return downloadLocation;
    }

    /**
     * Generates a unique flake id given cid and fid.
     * @param cid container's id on which this flake resides.
     * @param fid flake's id.
     * @return globally unique flake id.
     */
    public static String generateFlakeId(final String cid,
                                         final String fid) {
        return cid + "-" + fid;
    }

    /**
     * Various Constants used across the project.
     * Note: The string constants for configuration file,
     * and for the zookeeper paths are in config/ConfigProperties and
     * zookeeper/ZKConstants respectively. This file contains other generic
     * constants.
     */
    public static final class Constants {
        /**
         * The system file separator.
         */
        public static final String FILE_PATH_SEPARATOR = FloeConfig.getConfig()
                .getString(ConfigProperties.SYS_FILE_SEPARATOR);

        /**
         * The local execution mode for FLOE.
         */
        public static final String LOCAL = "local";

        /**
         * The distributed execution mode for FLOE.
         */
        public static final String DISTRIBUTED = "distributed";

        /**
         * Milli multiplier.
         */
        public static final int MILLI = 1000;

        /**
         * Chunk size to read from file and send to the coordinator.
         */
        public static final int BUFFER_SIZE = 1024 * 4;


        //public static final String FlakeReceiverFrontEndPrefix = ""

        /**
         * Flake receiver frontend prefix (this is suffixed by host and port).
         * Used for connecting to the predecessor flake.
         */
        public static final String FLAKE_RECEIVER_FRONTEND_CONNECT_SOCK_PREFIX
                = "tcp://";

        /**
         * Flake receiver backend prefix (this is suffixed by flake id).
         * Used for a PUSH socket to receive data from RECEIVER frontend and
         * send it to the pellets evenly.
         */
        public static final String FLAKE_RECEIVER_BACKEND_SOCK_PREFIX
                = "inproc://receiver-backend-";

        /**
         * Flake receiver Control socket prefix (this is suffixed by flake id).
         * Used for receiving control signals from the container.
         */
        public static final String FLAKE_RECEIVER_CONTROL_SOCK_PREFIX
                = "ipc://flake-control-";

        /**
         * Flake sender front-end prefix (this is suffixed by flake id).
         * Used for receiving data messages from all pellet instances.
         */
        public static final String FLAKE_SENDER_FRONTEND_SOCK_PREFIX
                = "inproc://sender-frontend-";

        /**
         * Flake sender middle-end prefix (this is suffixed by flake id).
         * Receives data messages from the front end. and uses PUB to
         * send it all outgoing channels based on dispersion
         * strategy (currently only duplicate it to all outgoing channels).
         */
        public static final String FLAKE_SENDER_MIDDLEEND_SOCK_PREFIX
                = "inproc://sender-middleend-";


        /**
         * Flake sender middle-end prefix (this is suffixed by a listening
         * port).
         * Receives data messages from the middle end. and uses PUSH to
         * send it to all flakes containing the pellet instances for the
         * succeeding pellet.
         */
        public static final String FLAKE_SENDER_BACKEND_SOCK_PREFIX
                = "tcp://*:";

        /**
         * the endpoint to be used by flakes to send their heartbeat to the
         * container.
         */
        public static final String FLAKE_HEARBEAT_SOCK_PREFIX
                = "ipc://flake-heartbeat-";

        /**
         * Number of i/o threads to be used by ZMQ for a single flake.
         */
        public static final int FLAKE_NUM_IO_THREADS = 4;

        /**
         * Apps folder name relative to container local folder.
         */
        public static final String CONTAINER_APP_FOLDER = "apps";
    }
}