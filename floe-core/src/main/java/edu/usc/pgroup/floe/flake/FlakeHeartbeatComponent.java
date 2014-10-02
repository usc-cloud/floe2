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

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * @author kumbhare
 */
public class FlakeHeartbeatComponent extends FlakeComponent {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FlakeHeartbeatComponent.class);

    /**
     * Container's unique id.
     */
    private final String containerId;

    /**
     * flake info.
     */
    private FlakeInfo finfo;

    /**
     * Constructor.
     * @param info the flake info object associated with this flake.
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     */
    public FlakeHeartbeatComponent(final FlakeInfo info,
                                   final String flakeId,
                                   final String componentName,
                                   final ZMQ.Context ctx) {
        super(flakeId, componentName, ctx);
        this.containerId = info.getContainerId();
        this.finfo = info;
    }

    /**
     * Starts all the sub parts of the given component and notifies when
     * components starts completely. This will be in a different thread,
     * so no need to worry.. block as much as you want.
     * @param terminateSignalReceiver socket which gets the terminate signal
     *                                when requested.
     */
    @Override
    protected final void runComponent(
            final ZMQ.Socket terminateSignalReceiver) {

        /**
         * ZMQ socket connection to the container.
         */
        ZMQ.Socket hsoc = getContext().socket(ZMQ.PUSH);
        String hbConnetStr = Utils.Constants.FLAKE_HEARBEAT_SOCK_PREFIX
                + containerId;
        LOGGER.info("Connecting to hb at: {}", hbConnetStr);
        hsoc.connect(hbConnetStr);

        long delay = FloeConfig.getConfig().getInt(ConfigProperties
                .FLAKE_HEARTBEAT_PERIOD) * Utils.Constants.MILLI;

        ZMQ.Poller pollerItems = new ZMQ.Poller(1);
        pollerItems.register(terminateSignalReceiver, ZMQ.Poller.POLLIN);

        notifyStarted(true);

        while (!Thread.currentThread().isInterrupted()) {
            finfo.updateUptime();
            LOGGER.debug("Sending HB: {}", finfo.getFlakeId());
            hsoc.send(Utils.serialize(finfo), 0);
            int polled = pollerItems.poll(delay);
            if (pollerItems.pollin(0)) {
                //terminate.
                LOGGER.warn("Terminating flake HB: {}", finfo.getFlakeId());
                terminateSignalReceiver.recv();
                finfo.setTerminated();
                hsoc.send(Utils.serialize(finfo), 0);
                break;
            }
        }

        hsoc.close();
        notifyStopped(true);
    }
}
