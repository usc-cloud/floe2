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

import edu.usc.pgroup.floe.app.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author kumbhare
 */
public abstract class FlakeLocalDispersionStrategy
        implements PelletUpdateListener {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FlakeLocalDispersionStrategy.class);

    /**
     * Backchannel sender specific to this edge.
     *
    private final BackChannelSenderComponent backChannelSenderComponent;*/

    /**
     * Initializes the strategy.
     * @param args the arguments sent by the user. Fix Me: make this a better
     *             interface.
     */
    public abstract void initialize(String args);

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     * @param tuple tuple object.
     * @param args custom arguments sent by the source flake with the tuple.
     * @return the list of target instances to send the given tuple.
     */
    public abstract String getTargetPelletInstance(Tuple tuple,
                                                   List<String> args);

    /**
     * Default constructor.
     */
    public FlakeLocalDispersionStrategy() {

    }
    /**
     * @return the current backchannel data (e.g. for loadbalancing or the
     * token on the ring etc.)
     */
    //public abstract byte[] getCurrentBackchannelData();

    /**
     * Constructor.
     * @param metricRegistry Metrics registry used to log various metrics.
     * @param context shared ZMQ context.
     * @param flakeId Current flake id.
     *
    public FlakeLocalDispersionStrategy(
            final MetricRegistry metricRegistry,
            final ZMQ.Context context,
            final String flakeId) {

        /*super(metricRegistry, flakeId, "FL-LOCAL-STRATEGY", context);

        LOGGER.info("Initializing flake local strategy.");
        backChannelSenderComponent = new BackChannelSenderComponent(
                metricRegistry, this,
                getFid(), "BACK-CHANNEL-SENDER", context);*
    }*/

    /**
     * Starts all the sub parts of the given component and notifies when
     * components starts completely. This will be in a different thread,
     * so no need to worry.. block as much as you want.
     * @param terminateSignalReceiver terminate signal receiver.
     *
    @Override
    protected final void runComponent(
            final ZMQ.Socket terminateSignalReceiver) {

        LOGGER.info("Starting fl local strategy.");
        //backChannelSenderComponent.startAndWait();
        //notifyStarted(true);

        terminateSignalReceiver.recv();

        LOGGER.info("Stopping back channel sender.");
        //backChannelSenderComponent.stopAndWait();
        //notifyStopped(true);
    }*/

    /**
     * Sends the tuple to the appropriate pellet.
     * @param from the middleend socket to retrieve the message
     * @param to the backend (PUB) socket to send it to (one or more) pellets.
     *
    public abstract void sendToPellets(final ZMQ.Socket from,
                                       final ZMQ.Socket to);*/
}

