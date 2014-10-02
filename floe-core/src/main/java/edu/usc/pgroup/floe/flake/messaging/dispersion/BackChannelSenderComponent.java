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
import edu.usc.pgroup.floe.flake.FlakeComponent;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * @author kumbhare
 */
public class BackChannelSenderComponent extends FlakeComponent {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(BackChannelSenderComponent.class);

    /**
     * Flake local dispersion strategy, which also decides what data should
     * be sent on the backchannel.
     */
    private final FlakeLocalDispersionStrategy dispersionStrategy;

    /**
     * Name of the source/predecessor pellet.
     */
    private final String srcPellet;

    /**
     * Constructor.
     * @param flakeLocalDispersionStrategy flake local strategy associated
     *                                     with this back channel.
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     * @param srcPelletName the pred. pellet name which is the src for this
     *                      edge.
     */
    public BackChannelSenderComponent(
              final FlakeLocalDispersionStrategy flakeLocalDispersionStrategy,
              final String flakeId,
              final String componentName,
              final ZMQ.Context ctx,
              final String srcPelletName) {
        super(flakeId, componentName, ctx);
        this.dispersionStrategy = flakeLocalDispersionStrategy;
        this.srcPellet = srcPelletName;
    }

    /**
     * Starts all the sub parts of the given component and notifies when
     * components starts completely. This will be in a different thread,
     * so no need to worry.. block as much as you want.
     *
     * @param terminateSignalReceiver terminate signal receiver.
     */
    @Override
    protected final void runComponent(
            final ZMQ.Socket terminateSignalReceiver) {
        LOGGER.info("Open back channel sender from flake");
        final ZMQ.Socket backendBackChannel = getContext().socket(ZMQ.PUB);

        backendBackChannel.connect(
                Utils.Constants.FLAKE_BACKCHANNEL_SENDER_PREFIX
                        + getFid());


        final ZMQ.Socket backChannelPingControl = getContext().socket(ZMQ.SUB);
        backChannelPingControl.subscribe("".getBytes());
        backChannelPingControl.connect(
                Utils.Constants.FLAKE_BACKCHANNEL_CONTROL_PREFIX
                        + getFid());

        int sleep = FloeConfig.getConfig().getInt(
                ConfigProperties.FLAKE_BACKCHANNEL_PERIOD);

        ZMQ.Poller pollerItems = new ZMQ.Poller(2);
        pollerItems.register(backChannelPingControl, ZMQ.Poller.POLLIN);
        pollerItems.register(terminateSignalReceiver, ZMQ.Poller.POLLIN);

        boolean done = false;

        notifyStarted(true);

        while (!done && !Thread.currentThread().interrupted()) {

            int polled = pollerItems.poll(sleep); //receive trigger.
            String toContinue = "1";


            byte[] pingData = new byte[]{1};
            if (pollerItems.pollin(0)) {
                pingData = backChannelPingControl.recv();
                LOGGER.info("Out of bound request to send bk channel msg "
                        + "received.");
                toContinue = "1";
            } else if (pollerItems.pollin(1)) { //backend
                pingData = terminateSignalReceiver.recv();
                toContinue = "0";
                done = true;
            }

            byte[] data = dispersionStrategy.getCurrentBackchannelData();
            LOGGER.debug("Sending backchannel msg for {}, {}, {}, {}.",
                    srcPellet, getFid(), data, toContinue);
            backendBackChannel.sendMore(srcPellet);
            backendBackChannel.sendMore(getFid());
            backendBackChannel.sendMore(toContinue);
            backendBackChannel.send(data, 0);
        }

        backendBackChannel.close();
        backChannelPingControl.close();

        notifyStopped(true);
    }
}
