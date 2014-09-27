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

package edu.usc.pgroup.floe.flake.messaging.dispersion;

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * @author kumbhare
 */
public class BackChannelSender extends Thread {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(BackChannelSender.class);

    /**
     * Shared zmq context.
     */
    private final ZMQ.Context ctx;

    /**
     * Shared zmq context.
     */
    private final String flakeId;


    /**
     * Constructor.
     * @param context Shared ZMQ Context.
     * @param fid Flake's id.
     */
    public BackChannelSender(final ZMQ.Context context, final String fid) {
        this.ctx = context;
        this.flakeId = fid;
    }

    /**
     * Backchannel's run function.
     */
    @Override
    public final void run() {
        LOGGER.info("Open back channel from pellet");
        final ZMQ.Socket backendBackChannel = ctx.socket(ZMQ.PUB);

        backendBackChannel.connect(
                Utils.Constants.FLAKE_BACKCHANNEL_PELLET_PROXY_PREFIX
                        + flakeId);

        while (!Thread.currentThread().interrupted()) {

            LOGGER.info("Sending backchannel msg.");
            backendBackChannel.sendMore(flakeId);
            backendBackChannel.send("ping".getBytes(), 0);

            try {
                Thread.currentThread().sleep(
                        FloeConfig.getConfig().getInt(ConfigProperties
                                .FLAKE_BACKCHANNEL_PERIOD)
                );
            } catch (InterruptedException e) {
                LOGGER.info("back channel interrupted");
            }
        }
    }
}
