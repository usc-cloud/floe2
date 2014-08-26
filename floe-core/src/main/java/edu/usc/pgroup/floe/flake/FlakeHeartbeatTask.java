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
import org.zeromq.ZMQ;

import java.util.TimerTask;

/**
 * The timer task for container heartbeat.
 *
 * @author kumbhare
 */
public class FlakeHeartbeatTask extends TimerTask {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FlakeHeartbeatTask.class);


    /**
     * flake info.
     */
    private FlakeInfo finfo;

    /**
     * ZMQ context used to send heartbeats to the container.
     */
    private ZMQ.Context zcontext;

    /**
     * ZMQ socket connection to the container.
     */
    private ZMQ.Socket hsoc;

    /**
     * Constructor.
     * @param info the flake info object associated with this flake.
     * @param context the shared ZMQ context.
     */
    FlakeHeartbeatTask(final FlakeInfo info,
                       final ZMQ.Context context) {
        this.finfo = info;
        this.zcontext = context;
        this.hsoc = null;
    }

    /**
     * Overriding the TimerTask's run method.
     * The heartbeat is recorded in the zookeeper.
     */
    @Override
    public final void run() {
        finfo.updateUptime();

        if (hsoc == null) {
            hsoc = zcontext.socket(ZMQ.PUSH);
            hsoc.connect(Utils.Constants.FLAKE_HEARBEAT_SOC);
        }

        //TODO: Update other flake information here.
        //send heartbeat via ZMQ.
        LOGGER.debug("Sending heartbeat: " + finfo.getFlakeId());
        hsoc.send(Utils.serialize(finfo), 0);
    }
}
