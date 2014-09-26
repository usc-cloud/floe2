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

import edu.usc.pgroup.floe.client.FloeClient;
import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.flake.FlakeInfo;
import edu.usc.pgroup.floe.resourcemanager.ResourceMapping;
import edu.usc.pgroup.floe.signals.ContainerSignal;
import edu.usc.pgroup.floe.thriftgen.TPellet;
import edu.usc.pgroup.floe.utils.RetryLoop;
import edu.usc.pgroup.floe.utils.RetryPolicyFactory;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.concurrent.Callable;

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
                    createOrUpdateFlakes(signal);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("Error occurred while creating flakes. Will "
                            + "abort");
                }
                break;
            case CONNECT_FLAKES:
                try {
                    connectFlakes(signal);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("Error occurred while creating flakes. Will "
                            + "abort");
                }
                break;
            case LAUNCH_PELLETS:
                try {
                    launchPellets(signal);
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
            case STOP_PELLETS:
                try {
                    stopPellets(signal);
                } catch (Exception e) {
                    e.printStackTrace();
                    LOGGER.error("Error occurred while launching pellets. Will "
                            + "abort");
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
            default:
                LOGGER.info("Unknown Signal type. Ignoring it.");
        }
    }

    /**
     * Sends signal to flakes to self terminate.
     * @param signal The signal (with associated data) received.
     * @exception Exception if an error occurs while terminating flakes.
     */
    private void terminateFlakes(final ContainerSignal signal)
            throws Exception {
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
            LOGGER.error("Could not receive resource mapping. Aborting.");
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

        LOGGER.info("terminating pellets.");
        Map<String, ResourceMapping.FlakeInstance> flakes
                = container.getFlakes();

        Map<String, FlakeInfo> pidToFidMap
                = FlakeMonitor.getInstance().getFlakes();

        for (Map.Entry<String, ResourceMapping.FlakeInstance> flakeEntry
                : flakes.entrySet()) {
            final String pid = flakeEntry.getKey();

            final String flakeId = pidToFidMap.get(pid).getFlakeId();
            ContainerUtils.sendKillFlakeCommand(flakeId);

            //while(FlakeMonitor.getInstance().getFlakes().containsKey(pid))
            try {
                Boolean killed = RetryLoop.callWithRetry(RetryPolicyFactory
                                .getDefaultPolicy(),
                        new Callable<Boolean>() {
                            @Override
                            public Boolean call() throws Exception {
                                FlakeInfo info = null;
                                try {
                                    info = FlakeMonitor.getInstance()
                                        .getFlakeInfo(pid);

                                } catch (Exception ex) {
                                    LOGGER.warn("Flake not found or already "
                                            + "terminated.");
                                }

                                if (info == null) {
                                    return true;
                                }
                                throw new Exception("Flake still alive. "
                                        + "Trying again. ");
                            }
                        });
                LOGGER.info("Flake terminated (at container):{}", flakeId);
            } catch (Exception e) {
                LOGGER.error("Could not kill flake in given time.");
            }
        }
        barrier.leave(); //finish launching pellets.
    }

    /**
     * Sends signal to flakes to start all pellets.
     * @param signal The signal (with associated data) received.
     * @exception Exception if an error occurs while stopping pellets.
     */
    private void stopPellets(final ContainerSignal signal) throws Exception {
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
            LOGGER.error("Could not receive resource mapping. Aborting.");
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

        LOGGER.info("Stopping pellets.");
        Map<String, ResourceMapping.FlakeInstance> flakes
                = container.getFlakes();

        Map<String, FlakeInfo> pidToFidMap
                = FlakeMonitor.getInstance().getFlakes();

        for (Map.Entry<String, ResourceMapping.FlakeInstance> flakeEntry
                : flakes.entrySet()) {
            String pid = flakeEntry.getKey();

            ContainerUtils.sendStopAppPelletsCommand(
                    pidToFidMap.get(pid).getFlakeId());
        }
        barrier.leave(); //finish launching pellets.
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
            LOGGER.error("Could not receive resource mapping. Aborting.");
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

    /**
     * Connects flakes with predecessor flakes based on the current resource
     * mapping.
     * @param signal The signal (with associated data) received.
     * @exception Exception if an error occurs while launching flakes.
     */
    private void connectFlakes(final ContainerSignal signal) throws Exception {
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
            LOGGER.error("Could not receive resource mapping. Aborting.");
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

        LOGGER.info("Sending connect signals.");
        barrier.enter(); //start processing.

        Map<String, ResourceMapping.FlakeInstance> flakes
                = container.getFlakes();

        Map<String, FlakeInfo> pidToFidMap
                = FlakeMonitor.getInstance().getFlakes();

        for (Map.Entry<String, ResourceMapping.FlakeInstance> flakeEntry
                : flakes.entrySet()) {

            String pid = flakeEntry.getKey();
            List<ResourceMapping.FlakeInstance> preds
                    = resourceMapping
                    .getPrecedingFlakes(pid);

            for (ResourceMapping.FlakeInstance pred: preds) {
                int assignedPort = pred.getAssignedPort(pid);
                int backPort = pred.getAssignedBackPort(pid);
                String host = pred.getHost();
                ContainerUtils.sendConnectCommand(
                        pidToFidMap.get(pid).getFlakeId(),
                        host, assignedPort, backPort);
            }
        }

        barrier.leave();
    }

    /**
     * Launches pellets in the flakes based on the current resource mapping.
     * @param signal The signal (with associated data) received.
     * @exception Exception if an error occurs while launching flakes.
     */
    private void launchPellets(final ContainerSignal signal) throws Exception {
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
            LOGGER.error("Could not receive resource mapping. Aborting.");
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

        LOGGER.info("Launching pellets.");
        Map<String, ResourceMapping.FlakeInstance> flakes
                = container.getFlakes();

        Map<String, FlakeInfo> pidToFidMap
                = FlakeMonitor.getInstance().getFlakes();

        for (Map.Entry<String, ResourceMapping.FlakeInstance> flakeEntry
                : flakes.entrySet()) {
            String pid = flakeEntry.getKey();

            ResourceMapping.FlakeInstance flakeInstance
                    = flakeEntry.getValue();

            TPellet pellet = resourceMapping.getFloeApp().get_pellets()
                    .get(flakeInstance.getCorrespondingPelletId());

            byte[] activeAlternate = pellet.get_alternates().get(
                    pellet.get_activeAlternate()
            ).get_serializedPellet();

            LOGGER.info("Creating {} instances.",
                    flakeInstance.getNumPelletInstances());
            for (int i = 0;
                 i < flakeInstance.getNumPelletInstances();
                 i++) {
                ContainerUtils.sendIncrementPelletCommand(
                        pidToFidMap.get(pid).getFlakeId(),
                        activeAlternate
                );
            }
        }
        barrier.leave(); //finish launching pellets.
    }

    /**
     * Creates, udpates or removes flakes from the container as required.
     * @param signal The signal (with associated data) received.
     * @exception Exception if an error occurs while launching flakes.
     */
    private void createOrUpdateFlakes(final ContainerSignal signal)
            throws Exception {

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
            LOGGER.error("Could not receive resource mapping. Aborting.");
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

        String applicationJar = resourceMapping.getApplicationJarPath();

        if (applicationJar != null) {
            try {

                String downloadLocation = Utils.getContainerJarDownloadPath(
                        resourceMapping.getAppName(), applicationJar);

                LOGGER.info("Downloading: " + applicationJar);
                FloeClient.getInstance().downloadFileSync(applicationJar,
                        downloadLocation);
                LOGGER.info("Finished Downloading: " + applicationJar);
            } catch (Exception e) {
                LOGGER.warn("No application jar specified. It should work"
                            + " still work for inproc testing. Exception: {}",
                            e);
            }
        }

        LOGGER.info("Resource Mapping {}: ", resourceMapping);

        Map<String, ResourceMapping.FlakeInstance> flakes
                = container.getFlakes();


        //Create and wait for flakes to respond.
        Map<String, String> pidToFidMap = ContainerUtils.createFlakes(
                resourceMapping.getAppName(),
                resourceMapping.getApplicationJarPath(),
                containerId,
                flakes);


        //NOTIFY THE COORDINATOR THAT THE CONTAINER IS DONE.
        //WE HAVE TO SETUP A MULTI-BARRIER (OR CHILDREN-BARRIER)
        barrier.leave(); //finish processing.
    }
}
