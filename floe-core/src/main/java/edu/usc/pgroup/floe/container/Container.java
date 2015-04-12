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

package edu.usc.pgroup.floe.container;

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.flake.FlakeInfo;
import edu.usc.pgroup.floe.resourcemanager.ResourceMapping;
import edu.usc.pgroup.floe.signals.ContainerSignal;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;
import java.util.Timer;

/**
 * @author kumbhare
 */
public final class Container {
    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(Container.class);

    /**
     * Recurring timer for sending heartbeats.
     */
    private Timer heartBeatTimer;

    /**
     * Apps assignment monitor.
     */
    //private AppsAssignmentMonitor appsAssignmentMonitor;

    /**
     * Signal monitor.
     */
    private SignalMonitor signalMonitor;

    /**
     * Flake monitor.
     */
    private FlakeMonitor flakeMonitor;


    /**
     * singleton container instance.
     */
    private static Container instance;

    /**
     * Hiding default constructor.
     */
    private Container() {

    }

    /**
     * @return the singleton container instance.
     */
    public static synchronized Container getInstance() {
        if (instance == null) {
            instance = new Container();
        }
        return instance;
    }

    /**
     * Starts the server and the schedules the heartbeats.
     */
    public void start() {
        LOGGER.info("Initializing Container.");
        initializeContainer();

        LOGGER.info("Scheduling container heartbeat.");
        scheduleHeartBeat(new ContainerHeartbeatTask());

        LOGGER.info("Starting application resource mapping monitor");
        startResourceMappingMonitor();


        LOGGER.info("Starting Signal Monitor.");
        startSignalMonitor();

        LOGGER.info("Starting flake Monitor");
        LOGGER.info("Flake port range start: {}", FloeConfig.getConfig().getInt(
                ConfigProperties.FLAKE_RECEIVER_PORT
        ));
        startFlakeMonitor();

        LOGGER.info("Container Started.");
    }

    /**
     * Starts the flake monitor, listening for flake heartbeats.
     */
    private void startFlakeMonitor() {
        flakeMonitor = FlakeMonitor.getInstance();
        flakeMonitor.initialize(ContainerInfo.getInstance().getContainerId());
        flakeMonitor.startMonitor();
    }


    /**
     * Starts the curator cache for monitoring the signals.
     */
    private void startSignalMonitor() {
        signalMonitor = new SignalMonitor(ContainerInfo
                .getInstance().getContainerId());
    }

    /**
     * Starts the curator cache for monitoring the applications,
     * and the pellets assigned to this container.
     */
    private void startResourceMappingMonitor() {
       //appsAssignmentMonitor = new AppsAssignmentMonitor(ContainerInfo
         //       .getInstance().getContainerId());
    }

    /**
     * Schedules and starts recurring the container heartbeat.
     *
     * @param containerHeartbeatTask Heartbeat timer task.
     */
    private void scheduleHeartBeat(final ContainerHeartbeatTask
                                           containerHeartbeatTask) {
        if (heartBeatTimer == null) {
            heartBeatTimer = new Timer();
            heartBeatTimer.scheduleAtFixedRate(containerHeartbeatTask
                    , 0
                    , FloeConfig.getConfig().getInt(ConfigProperties
                    .CONTAINER_HEARTBEAT_PERIOD) * Utils.Constants.MILLI);
            LOGGER.info("Heartbeat scheduled with period: "
                    + FloeConfig.getConfig().getInt(
                    ConfigProperties.CONTAINER_HEARTBEAT_PERIOD)
                    + " seconds");
        }
    }

    /**
     * Initializes the container. Including:
     * setup the containerInfo object (for heartbeat)
     * TODO: PerfInfoObject etc.
     */
    private void initializeContainer() {
        ContainerInfo.getInstance().setStartTime(new Date().getTime());
    }

    /**
     * Processes the signal received (from coordinator or other containers).
     * @param signal the received signal to process.
     */
    public void processSignal(final ContainerSignal signal) {
        switch (signal.getSignalType()) {
            case CREATE_FLAKES:
                try {
                    createFlakes(signal);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("Error occurred while creating flakes. Will "
                            + "abort");
                }
                break;
            case INITIALIZE_FLAKES:
                try {
                    initializeFlakes(signal);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("Error occurred while Initializing flakes. "
                            + " Will abort");
                }
                break;
            case TERMINATE_FLAKES:
                try {
                    terminateFlakes(signal);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("Error occurred while launching pellets. Will "
                            + "abort");
                }
                break;
            case CONNECT_OR_DISCONNECT_FLAKES:
                try {
                    connectFlakes(signal);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("Error occurred while creating flakes. Will "
                            + "abort");
                }
                break;
            case INCREASE_OR_DECREASE_PELLETS:
                try {
                    increaseOrDecreasePellets(signal);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("Error occurred while launching pellets. Will "
                            + "abort");
                }
                break;
            case START_PELLETS:
                try {
                    startPellets(signal);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("Error occurred while launching pellets. Will "
                            + "abort");
                }
                break;
            default:
                LOGGER.info("Unknown Signal type. Ignoring it.");
        }
    }

    /**
     * creates or terminates pellet instances within a flake.
     * @param signal The signal (with associated data) received.
     * @throws Exception If an error occurs while performing the action.
     */
    private void increaseOrDecreasePellets(final ContainerSignal signal)
            throws Exception {

        String appName = signal.getDestApp();

        ResourceMapping resourceMapping
                = ZKUtils.getResourceMapping(appName);

        String appUpdateBarrierPath = ZKUtils
                .getApplicationBarrierPath(appName);

        int numContainersToUpdate = resourceMapping.getContainersToUpdate();

        DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(
                ZKClient.getInstance().getCuratorClient(),
                appUpdateBarrierPath,
                numContainersToUpdate + 1
        );

        if (ContainerActions.isContainerUpdated(resourceMapping)) {
            barrier.enter();
            try {
                ContainerActions.increaseOrDecreasePellets(resourceMapping);
            } catch (Exception e) {
                LOGGER.error("Could not launch pellets. Exception {}", e);
            } finally {
                barrier.leave();
            }
        }
    }

    /**
     * creates flakes on the container.
     * @param signal The signal (with associated data) received.
     * @throws Exception If an error occurs while performing the action.
     */
    private void connectFlakes(final ContainerSignal signal) throws Exception {

        String appName = signal.getDestApp();

        ResourceMapping resourceMapping
                = ZKUtils.getResourceMapping(appName);

        String appUpdateBarrierPath = ZKUtils
                .getApplicationBarrierPath(appName);

        int numContainersToUpdate = resourceMapping.getContainersToUpdate();

        DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(
                ZKClient.getInstance().getCuratorClient(),
                appUpdateBarrierPath,
                numContainersToUpdate + 1
        );

        if (ContainerActions.isContainerUpdated(resourceMapping)) {
            barrier.enter();
            try {
                ContainerActions.updateFlakeConnections(resourceMapping);
            } catch (Exception e) {
                LOGGER.error("Could not create flakes. Exception {}", e);
            } finally {
                barrier.leave();
            }
        }
    }

    /**
     * terminates required flakes from the container. (assuming all the pellets
     * are already terminated).
     * @param signal The signal (with associated data) received.
     * @throws Exception If an error occurs while performing the action.
     */
    private void terminateFlakes(final ContainerSignal signal)
            throws Exception {

        String appName = signal.getDestApp();

        ResourceMapping resourceMapping
                = ZKUtils.getResourceMapping(appName);

        String appUpdateBarrierPath = ZKUtils
                .getApplicationBarrierPath(appName);

        int numContainersToUpdate = resourceMapping.getContainersToUpdate();

        DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(
                ZKClient.getInstance().getCuratorClient(),
                appUpdateBarrierPath,
                numContainersToUpdate + 1
        );

        if (ContainerActions.isContainerUpdated(resourceMapping)) {
            barrier.enter();
            try {
                ContainerActions.terminateFlakes(resourceMapping);
            } catch (Exception e) {
                LOGGER.error("Could not terminate flakes. Exception {}", e);
            } finally {
                barrier.leave();
            }
        }
    }

    /**
     * initializes newly created flakes on the container.
     * @param signal The signal (with associated data) received.
     * @throws Exception If an error occurs while performing the action.
     */
    private void initializeFlakes(final ContainerSignal signal)
            throws Exception {

        String appName = signal.getDestApp();

        ResourceMapping resourceMapping
                = ZKUtils.getResourceMapping(appName);

        String appUpdateBarrierPath = ZKUtils
                .getApplicationBarrierPath(appName);

        int numContainersToUpdate = resourceMapping.getContainersToUpdate();

        DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(
                ZKClient.getInstance().getCuratorClient(),
                appUpdateBarrierPath,
                numContainersToUpdate + 1
        );

        if (ContainerActions.isContainerUpdated(resourceMapping)) {
            barrier.enter();
            try {
                ContainerActions.initializeFlakes(resourceMapping);
            } catch (Exception e) {
                LOGGER.error("Could not create flakes. Exception {}", e);
            } finally {
                barrier.leave();
            }
        }
    }

    /**
     * creates new flakes on the container.
     * @param signal The signal (with associated data) received.
     * @throws Exception If an error occurs while performing the action.
     */
    private void createFlakes(final ContainerSignal signal) throws Exception {

        String appName = signal.getDestApp();

        ResourceMapping resourceMapping
                = ZKUtils.getResourceMapping(appName);

        String appUpdateBarrierPath = ZKUtils
                .getApplicationBarrierPath(appName);

        int numContainersToUpdate = resourceMapping.getContainersToUpdate();

        DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(
                ZKClient.getInstance().getCuratorClient(),
                appUpdateBarrierPath,
                numContainersToUpdate + 1
        );

        if (ContainerActions.isContainerUpdated(resourceMapping)) {
            barrier.enter();
            try {
                ContainerActions.createFlakes(resourceMapping);
            } catch (Exception e) {
                LOGGER.error("Could not create flakes. Exception {}", e);
            } finally {
                barrier.leave();
            }
        }
    }


    /**
     * Sends signal to flakes to start all pellets.
     * mapping.
     * @param signal The signal (with associated data) received.
     * @exception Exception if an error occurs while launching flakes.
     */
    private void startPellets(final ContainerSignal signal) throws Exception {
        String appName = signal.getDestApp();
        String containerName = signal.getDestContainer(); //ignore.
        byte[] data = signal.getSignalData();

        LOGGER.info("Container Id: " + containerName);

        String resourceMappingPath = ZKUtils
                .getApplicationResourceMapPath(appName);

        byte[] serializedRM = null;

        try {
            serializedRM = ZKClient.getInstance().getCuratorClient().getData()
                    .forPath(resourceMappingPath);
        } catch (Exception e) {
            LOGGER.error("Could not receive resource mapping to start pellets");
            return;
        }

        ResourceMapping resourceMapping =
                (ResourceMapping) Utils.deserialize(serializedRM);

        String containerId = ContainerInfo.getInstance().getContainerId();

        ResourceMapping.ContainerInstance container
                = resourceMapping.getContainer(containerId);

        if (container == null) {
            LOGGER.info("No resource mapping for this container.");
            return;
        }

        String appUpdateBarrierPath = ZKUtils
                .getApplicationBarrierPath(appName);


        int numContainersToUpdate = resourceMapping.getContainersToUpdate();

        DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(
                ZKClient.getInstance().getCuratorClient(),
                appUpdateBarrierPath,
                numContainersToUpdate + 1
        );

        LOGGER.info("Entering barrier: " + appUpdateBarrierPath);
        barrier.enter(); //start processing.

        LOGGER.info("Starting pellets.");
        Map<String, ResourceMapping.FlakeInstance> flakes
                = container.getFlakes();

        Map<String, FlakeInfo> pidToFidMap
                = FlakeMonitor.getInstance().getFlakes();

        for (Map.Entry<String, ResourceMapping.FlakeInstance> flakeEntry
                : flakes.entrySet()) {
            String pid = flakeEntry.getKey();

            ContainerUtils.sendStartPelletsCommand(
                    pidToFidMap.get(pid).getFlakeId());
        }
        barrier.leave(); //finish launching pellets.
    }
}
