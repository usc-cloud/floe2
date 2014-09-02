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

package edu.usc.pgroup.floe.flake;

import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Date;

/**
 * The Container info object which is sent with each heartbeat.
 * Note: this does not include perf. details, we use a separate entity for
 * all perf related information per container which is usually updated less
 * often.
 *
 * @author kumbhare
 */
public final class FlakeInfo implements Serializable {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FlakeInfo.class);

    /**
     * Container id.
     */
    private final String containerId;

    /**
     * Application name.
     */
    private final String appName;

    /**
     * A container-local unique flake id.
     */
    private String flakeId;

    /**
     * The time at which this container was last started.
     */
    private long startTime;

    /**
     * Time for which this container has been running (since last restart).
     */
    private long uptime;

    /**
     * Pellet id (same as pellet name).
     */
    private String pelletId;

    /**
     * flag to indicate if the flake has been terminated.
     */
    private boolean terminated;


    /**
     * private constructor that sets the constant or default values. Hidden
     * from others since this is a singleton class.
     * @param pid pellet id (same as pellet name)
     * @param fid Flake's id.
     * @param cid Container id.
     * @param app application name to which this pellet belongs.
     */
    public FlakeInfo(final String pid, final String fid, final String cid,
                     final String app) {
        this.pelletId = pid;
        this.flakeId = fid;
        this.containerId = cid;
        this.appName = app;
        this.terminated = false;
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
     * @return flake's id
     */
    public String getFlakeId() {
        return flakeId;
    }

    /**
     * @return container id.
     */
    public String getContainerId() {
        return containerId;
    }

    /**
     * @return pellet id.
     */
    public String getPelletId() {
        return pelletId;
    }

    /**
     * @return application name.
     */
    public String getAppName() { return appName; }

    /**
     * @return true if the flake has been marked for termination.
     */
    public boolean isTerminated() { return terminated; }

    /**
     * Marks the flake as terminated, which will be used by the container to
     * cleanup.
     */
    public void setTerminated() { this.terminated = true; }
}
