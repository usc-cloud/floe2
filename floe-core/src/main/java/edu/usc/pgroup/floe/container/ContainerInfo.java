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
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The Container info object which is sent with each heartbeat.
 * Note: this does not include perf. details, we use a separate entity for
 * all perf related information per container which is usually updated less
 * often.
 *
 * @author kumbhare
 */
public final class ContainerInfo implements Serializable {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ContainerInfo.class);

    /**
     * singleton container info instance.
     */
    private static ContainerInfo instance;

    /**
     * Number of processors on the machine hosting this container.
     */
    private final int numCores;

    /**
     * Number of cores currently being used (we assume 1 pellet = 1 core).
     */
    private int usedCores;

    /**
     * A unique container id.
     */
    private String containerId;

    /**
     * Host name or ip address that will be used for connecting to the flakes.
     * hosted by this container.
     */
    private String hostnameOrIpAddr;

    /**
     * The time at which this container was last started.
     */
    private long startTime;

    /**
     * Time for which this container has been running (since last restart).
     */
    private long uptime;

    /**
     * The starting port number which is to be used for creating bind sockets
     * for different flakes on this container.
     */
    private int portRangeStart;


    /**
     * List of flakes running on this container. Pelletid to flakeInfo map.
     */
    private final Map<String, FlakeInfo> currentFlakes;


    /**
     * private constructor that sets the constant or default values. Hidden
     * from others since this is a singleton class.
     */
    private ContainerInfo() {
        //TODO: Get the container id if it exists in local state. (This will
        // be useful if a container is restarted). For now, generating a UUID.
        containerId = UUID.randomUUID().toString();

        hostnameOrIpAddr = Utils.getHostNameOrIpAddress();

        portRangeStart = FloeConfig.getConfig().getInt(
                ConfigProperties.FLAKE_RECEIVER_PORT
        );

        this.numCores = Runtime.getRuntime().availableProcessors();

        this.uptime = 0;
        this.currentFlakes = new HashMap<>();
    }

    /**
     * @return the singleton containerInfo Instance.
     */
    public static synchronized ContainerInfo getInstance() {
        if (instance == null) {
            instance = new ContainerInfo();
        }
        return instance;
    }

    /**
     * @return the unique container id
     */
    public String getContainerId() {
        return containerId;
    }

    /**
     * @return the hostname or ipaddr to be used for connecting to flakes on
     * this container.
     */
    public String getHostnameOrIpAddr() {
        return hostnameOrIpAddr;
    }

    /**
     * Sets the time at which the container was started.
     *
     * @param containerStartTime the start time in milliseconds
     */
    public void setStartTime(final long containerStartTime) {
        this.startTime = containerStartTime;
    }

    /**
     * updates the uptime for this container.
     */
    public void updateUptime() {
        this.uptime = new Date().getTime() - this.startTime;
        LOGGER.debug("Updating uptime: " + this.uptime / Utils.Constants.MILLI
                + " Seconds");
    }

    /**
     * @return Time for which this container has been up (since last restart).
     */
    public long getUptime() {
        return uptime;
    }

    /**
     * @return  The starting port number which is to be used for
     * creating bind sockets for different flakes on this container.
     */
    public int getPortRangeStart() { return portRangeStart; }

    /**
     * Updates the list of flakes with current data.
     * @param flakes list of flakes from the flake monitor. Pelletid to flake
     *               info map.
     */
    public void updateFlakes(final Map<String, FlakeInfo> flakes) {
        if (flakes != null) {
            currentFlakes.clear();
            currentFlakes.putAll(flakes);
            usedCores = 0;
            for (FlakeInfo fl : flakes.values()) {
                usedCores += fl.getNumPellets();
            }
        }
    }

    /**
     * @return a map from pelletid to Flakeinfo for all flakes running on
     * this container.
     */
    public Map<String, FlakeInfo> getCurrentFlakes() {
        return currentFlakes;
    }

    /**
     * @return number of cores on the machine hosting this container.
     */
    public int getNumCores() {
        return numCores;
    }

    /**
     * @return return the number of currently used cores.
     */
    public int getUsedCores() {
        return usedCores;
    }

    /**
     * @return number of currently available cores.
     */
    public int getAvailableCores() {
        return getNumCores() - getUsedCores();
    }
}
