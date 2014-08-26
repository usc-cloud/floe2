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
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Timer;

/**
 * @author kumbhare
 */
public class Container {
    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ContainerService.class);

    /**
     * Recurring timer for sending heartbeats.
     */
    private Timer heartBeatTimer;

    /**
     * the containerInfo object sent during heartbeats.
     */
    private ContainerInfo containerInfo;


    /**
     * Apps assignment monitor.
     */
    private AppsAssignmentMonitor appsAssignmentMonitor;


    /**
     * Flake monitor.
     */
    private FlakeMonitor flakeMonitor;

    /**
     * Starts the server and the schedules the heartbeats.
     */
    public final void start() {
        LOGGER.info("Initializing Container.");
        initializeContainer();

        LOGGER.info("Scheduling container heartbeat.");
        scheduleHeartBeat(new ContainerHeartbeatTask());

        LOGGER.info("Starting application resource mapping monitor");
        startResourceMappingMonitor();

        LOGGER.info("Starting flake Monitor");
        startFlakeMonitor();

        LOGGER.info("Container Started.");
    }

    /**
     * Starts the flake monitor, listening for flake heartbeats.
     */
    private void startFlakeMonitor() {
        flakeMonitor = FlakeMonitor.getInstance();
    }

    /**
     * Starts the curator cache for monitoring the applications,
     * and the pellets assigned to this container.
     */
    private void startResourceMappingMonitor() {
       appsAssignmentMonitor = new AppsAssignmentMonitor(ContainerInfo
                .getInstance().getContainerId());
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
        containerInfo = ContainerInfo.getInstance();
        containerInfo.setStartTime(new Date().getTime());
    }
}
