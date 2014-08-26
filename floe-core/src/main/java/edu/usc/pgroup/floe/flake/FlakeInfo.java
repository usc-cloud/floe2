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
     * private constructor that sets the constant or default values. Hidden
     * from others since this is a singleton class.
     * @param fid Flake's id
     */
    public FlakeInfo(final String fid) {
        this.flakeId = fid;
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
}
