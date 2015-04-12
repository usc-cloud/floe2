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

package edu.usc.pgroup.floe.flake.messaging.sender;

import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.flake.FlakeComponent;
import edu.usc.pgroup.floe.thriftgen.TChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author kumbhare
 */
public class SenderFEComponent extends FlakeComponent {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SenderFEComponent.class);

    /**
     * the map of pellet to ports to start the zmq sockets.
     * one for each edge in the application graph.
     */
    private final Map<String, Integer> pelletPortMap;


    /**
     * the map of pellet to ports to start the zmq sockets for the dispersion.
     */
    private final Map<String, Integer> pelletBackChannelPortMap;


    /**
     * the map of pellet to list of streams that pellet is subscribed to.
     */
    private final Map<String, List<String>> pelletStreamsMap;

    /**
     * Map of target pellet to channel type (one per edge).
     */
    private final Map<String, TChannel> pelletChannelTypeMap;

    /**
     * Pellet's name to be sent with each message.
     */
    private final String myPelletName;

    /**
     * Application name.
     */
    private final String appName;

    /**
     * constructor.
     * @param metricRegistry Metrics registry used to log various metrics.
     * @param ctx Shared ZMQ context.
     * @param app Application name.
     * @param pelletName Pellet's name to be sent with each message.
     * @param flakeId flake id to which this sender belongs.
     * @param componentName Component name.
     * @param portMap the map of ports on which this flake should
     *                       listen on. Note: This is fine here (and not as a
     *                       control signal) because this depends only on
     *                       static application configuration and not on
     * @param backChannelPortMap ports for dispersion.
     * @param channels Map of target pellet to channel type (one per edge)
     * @param streamsMap map from successor pellets to subscribed
     *                         streams.
     */
    public SenderFEComponent(final MetricRegistry metricRegistry,
                             final ZMQ.Context ctx,
                               final String app,
                               final String pelletName,
                               final String flakeId,
                               final String componentName,
                               final Map<String, Integer> portMap,
                               final Map<String, Integer> backChannelPortMap,
                               final Map<String, TChannel> channels,
                               final Map<String, List<String>> streamsMap) {
        super(metricRegistry, flakeId, componentName, ctx);
        this.appName = app;
        this.myPelletName = pelletName;
        this.pelletPortMap = portMap;
        this.pelletBackChannelPortMap = backChannelPortMap;
        this.pelletChannelTypeMap = channels;
        this.pelletStreamsMap = streamsMap;
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
        //new SenderMEComponent().start
        SenderMEComponent me = new SenderMEComponent(getMetricRegistry(),
                getFid(), "ME", getContext());

        me.startAndWait();

        List<SenderBEComponent> bes = new ArrayList<>();

        for (String pellet: pelletPortMap.keySet()) {
            int port = pelletPortMap.get(pellet);
            int bpPort = pelletBackChannelPortMap.get(pellet);
            TChannel channel = pelletChannelTypeMap.get(pellet);
            List<String> streams = pelletStreamsMap.get(pellet);

            SenderBEComponent be
                    = new SenderBEComponent(getMetricRegistry(),
                    getFid(), "BE-"+pellet, getContext(),
                    port, bpPort, appName, pellet,
                    channel, streams, myPelletName);

            be.startAndWait();
            bes.add(be);
        }

        notifyStarted(true);
        terminateSignalReceiver.recv(); //wait for the terminate signal.

        me.stopAndWait();
        for (SenderBEComponent be: bes) {
            be.stopAndWait();
        }
        notifyStopped(true);
    }
}
