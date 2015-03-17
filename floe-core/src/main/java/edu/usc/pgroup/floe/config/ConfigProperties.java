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

package edu.usc.pgroup.floe.config;

/**
 * configuration property names and types.
 *
 * @author Alok Kumbhare
 */
public final class ConfigProperties {

    /**
     * Execution mode for floe (local or distributed).
     */
    public static final String FLOE_EXEC_MODE = "floe.execution.mode";

    /**
     * FLOE scratch folder, used by different components locally.
     * Relative to FLOE_HOME
     */
    public static final String FLOE_EXEC_SCRATCH_FOLDER = "floe.execution"
            + ".folder.scratch";

    /**
     * FLOE jar (with all dependencies) containing all components of the
     * application.
     */
    public static final String FLOE_EXE_JAR = "floe.execution.jar";

    /**
     * Floe resource manager.
     */
    public static final String FLOE_EXE_RESOURCE_MANAGER = "floe.execution"
            + ".resourcemanager";

    /**
     * The host address for the FLOE coordinator.
     */
    public static final String COORDINATOR_HOST = "floe.coordinator.host";

    /**
     * The port on which the coordinator is listening.
     */
    public static final String COORDINATOR_PORT = "floe.coordinator.port";


    /**
     * The number of threads in the coordinator coordinator thread pool.
     */
    public static final String COORDINATOR_SERVICE_THREAD_COUNT =
            "floe.coordinator.threadcount";

    /**
     * COORDINATOR File upload folder.
     * Relative to FLOE_EXEC_SCRATCH_FOLDER
     */
    public static final String COORDINATOR_FILE_UPLOAD_FOLDER = "floe"
            + ".coordinator.folder.fileupload";

    /**
     * Retry policy used by the resource manager while communicating with the
     * resources.
     */
    public static final String RESOURCEMANAGER_RETRYPOLICY = "floe"
            + ".resourcemanager.retrypolicy";

    /**
     * Arguments to the retry policy.
     */
    public static final java.lang.String RESOURCEMANAGER_RETRYPOLICY_ARGS
            = "floe.resourcemanager.retrypolicy.args";

    /**
     * Heartbeat period for the container.
     */
    public static final String CONTAINER_HEARTBEAT_PERIOD = "floe.container"
            + ".heartbeat.period";


    /**
     * Container local folder. Used by both container and flakes.
     * Relative to FLOE_EXEC_SCRATCH_FOLDER.
     */
    public static final String CONTAINER_LOCAL_FOLDER = "floe.container.local"
            + ".folder";


    /**
     * Heartbeat period for the container.
     */
    public static final String FLAKE_HEARTBEAT_PERIOD = "floe.flake.heartbeat"
            + ".period";


    /**
     * Flake state checkpoint period.
     */
    public static final String FLAKE_STATE_CHECKPOINT_PERIOD
            = "floe.flake.statecheckpoint.period";

    /**
     * FLAKE (re)launch delay.
     */
    public static final java.lang.String FLAKE_LAUNCH_DELAY = "floe.fake"
            + ".launch.delay";


    /**
     * Flake's local folder.
     * Relative to CONTAINER_LOCAL_FOLDER
     */
    public static final String FLAKE_LOCAL_FOLDER = "floe.flake.local"
            + ".folder";


    /**
     * Start of the Port range for the flakes to listen on.
     */
    public static final String FLAKE_RECEIVER_PORT = "floe.flake.port";

    /**
     * The period of sending backchannel signals.
     */
    public static final String FLAKE_BACKCHANNEL_PERIOD
            = "floe.flake.backchannel.period";

    /**
     * The tolerance level (and replication factor) for the flakes.
     */
    public static final java.lang.String FLAKE_TOLERANCE_LEVEL
            = "floe.flake.tolerance.level";

    /**
     * The tuple serializer plugin.
     */
    public static final java.lang.String TUPLE_SERIALIZER = "floe.tuple"
            + ".serializer";

    /**
     * Hostname to be used for connecting to this server.
     */
    public static final String HOST_NAME = "floe.container.hostname";

    /**
     * List of Zookeeper servers.
     */
    public static final String ZK_SERVERS = "floe.zk.servers";

    /**
     * ZK Server port.
     */
    public static final String ZK_PORT = "floe.zk.port";

    /**
     * Zookeeper root folder for FLOE.
     * (useful if the same zookeeper ensemble is used for multiple clients).
     */
    public static final String ZK_ROOT = "floe.zk.root";

    /**
     * ZK base retry timeout duration for exponential backoff algorithm.
     */
    public static final String ZK_RETRY_TIMEOUT = "floe.zk.retry.timeout";

    /**
     * ZK number of connection retries.
     * After which a new ZK server is selected or a failure is reported.
     */
    public static final String ZK_RETRY_ATTEMPTS = "floe.zk.retry.attempts";

    /**
     * This and following are all the SYSTEM inherited properties.
     * These do not appear in the .properties file but are inherited from the
     * environment.
     * System File Separator '/' in most cases
     */
    public static final String SYS_FILE_SEPARATOR = "file.separator";

    /**
     * To allow user to override the config file from command line.
     * TODO: Incoroporate this later.
     */
    public static final String SYS_CONFIG_FILE = "floe.config.file";

    /**
     * System property for pointing to JAVA_HOME.
     */
    public static final String SYS_JAVA_HOME = "java.home";

    /**
     * System property for pointing to JAVA LIBRARY path.
     */
    public static final String SYS_JAVA_LIB_PATH = "java.library.path";

    /**
     * System property for pointing to JAVA LIBRARY path.
     */
    public static final String SYS_JAVA_CLASS_PATH = "java.class.path";

    /**
     * System property for pointing to Path separator.
     */
    public static final String SYS_PATH_SEPARATOR = "path.separator";


    /**
     * Channel/dispersion classes for reducer
     */
    public static final String FLAKE_REDUCER_DISPERSION = "floe.flake"
            + ".messaging.dispersion.reducer";

    /**
     * Channel/dispersion classes for reducer (local)
     */
    public static final String FLAKE_REDUCER__LOCAL_DISPERSION = "floe.flake"
            + ".messaging.dispersion.reducer.local";

    /**
     * Channel/dispersion classes for round robin
     */
    public static final String FLAKE_RR_DISPERSION = "floe.flake"
            + ".messaging.dispersion.rr";

    /**
     * Channel/dispersion classes for reducer (local)
     */
    public static final String FLAKE_RR_LOCAL_DISPERSION = "floe.flake"
            + ".messaging.dispersion.rr.local";

    /**
     * Hiding the default constructor.
     */
    private ConfigProperties() {

    }
}
