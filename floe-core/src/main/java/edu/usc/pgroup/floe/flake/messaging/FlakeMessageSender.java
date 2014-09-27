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

package edu.usc.pgroup.floe.flake.messaging;

import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.flake.messaging.dispersion.MessageDispersionStrategy;
import edu.usc.pgroup.floe.flake.messaging.dispersion
        .MessageDispersionStrategyFactory;
import edu.usc.pgroup.floe.serialization.SerializerFactory;
import edu.usc.pgroup.floe.serialization.TupleSerializer;
import edu.usc.pgroup.floe.thriftgen.TChannelType;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

/**
 * @author kumbhare
 */
public class FlakeMessageSender extends Thread {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FlakeMessageSender.class);

    /**
     * The ZMQ context.
     */
    private final ZMQ.Context ctx;

    /**
     * the id of the flake to which this sender belongs.
     */
    private final String fid;

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
    private final Map<String, String> pelletChannelTypeMap;

    /**
     * Pellet's name to be sent with each message.
     */
    private final String myPelletName;

    /**
     * constructor.
     * @param zmqContext Shared ZMQ context.
     * @param pelletName Pellet's name to be sent with each message.
     * @param flakeId flake id to which this sender belongs.
     * @param portMap the map of ports on which this flake should
     *                       listen on. Note: This is fine here (and not as a
     *                       control signal) because this depends only on
     *                       static application configuration and not on
     * @param backChannelPortMap ports for dispersion.
     * @param channelTypeMap Map of target pellet to channel type (one per edge)
     * @param streamsMap map from successor pellets to subscribed
     *                         streams.
     */
    public FlakeMessageSender(final ZMQ.Context zmqContext,
                              final String pelletName,
                              final String flakeId,
                              final Map<String, Integer> portMap,
                              final Map<String, Integer> backChannelPortMap,
                              final Map<String, String> channelTypeMap,
                              final Map<String, List<String>> streamsMap) {
        this.myPelletName = pelletName;
        this.ctx = zmqContext;
        this.fid = flakeId;
        this.pelletPortMap = portMap;
        this.pelletBackChannelPortMap = backChannelPortMap;
        this.pelletChannelTypeMap = channelTypeMap;
        this.pelletStreamsMap = streamsMap;
    }

    /**
     * This is used to start the proxy from tcp socket to the pellets.
     */
    public final void run() {
        new MiddleEnd().start();
        for (String pellet: pelletPortMap.keySet()) {
            int port = pelletPortMap.get(pellet);
            int bpPort = pelletBackChannelPortMap.get(pellet);
            List<String> streams = pelletStreamsMap.get(pellet);
            String channelType = pelletChannelTypeMap.get(pellet);
            new BackEnd(port, bpPort, channelType, streams).start();
        }
    }

    /**
     * The backend class. An instance of this is created per edge in the app
     * graph. Each backend listens on a single port for connections and is
     * responsible for sending data from the middleend to that port.
     */
    private class BackEnd extends Thread {

        /**
         * port on which the back end should listen for
         *             connections from downstream.
         */
        private final int port;


        /**
         * port on which the back end should listen for connection on the
         * dispersion. This dispersion will be used for send client
         * identities and other channel control information.
         */
        private final int backChannelPort;

        /**
         * Stream names this backend should subscribe to.
         */
        private final List<String> streamNames;


        /**
         * Message dispersion strategy to be used for the channel.
         */
        private MessageDispersionStrategy dispersionStrategy;

        /**
         * Serializer to be used to serialize and deserialize the data tuples.
         */
        private final TupleSerializer tupleSerializer;

        /**
         * constructor.
         * @param p port on which the back end should listen for
         *             connections from downstream.
         * @param bp dispersion port.
         * @param channelType channel type (e.g. round robin, reduce,
         *                    load balanced, custom)
         * @param streams list of stream names to subscribe to.
         */
        public BackEnd(final int p, final int bp,
                       final String channelType, final List<String> streams) {
            this.port = p;
            this.backChannelPort = bp;
            this.streamNames = streams;

            this.tupleSerializer = SerializerFactory.getSerializer();


            String[] ctypesAndArgs = channelType.split("__");
            String ctype = ctypesAndArgs[0];
            String args = null;
            if (ctypesAndArgs.length > 1) {
                args = ctypesAndArgs[1];
            }
            LOGGER.info("type and args: {}, Channel type: {}", channelType,
                    ctype);

            this.dispersionStrategy = null;

            if (!ctype.startsWith("NONE")) {
                TChannelType type = Enum.valueOf(TChannelType.class, ctype);
                try {
                    this.dispersionStrategy = MessageDispersionStrategyFactory
                            .getMessageDispersionStrategy(type, args);
                } catch (Exception ex) {
                    LOGGER.error("Invalid dispersion strategy: {}. "
                            + "Using default RR", type);
                }
            }

        }

        /**
         * Backend thread's run method. This is responsible for the routing.
         */
        public void run() {

            final ZMQ.Socket middleendreceiver  = ctx.socket(ZMQ.SUB);

            middleendreceiver.connect(
                    Utils.Constants.FLAKE_SENDER_MIDDLEEND_SOCK_PREFIX + fid);

            //subscribe for all streams requested by the user.
            if (streamNames != null) {
                for (String streamName : streamNames) {
                    LOGGER.info("Subscribing: {}", streamName);
                    middleendreceiver.subscribe(streamName.getBytes());
                }
            }

            final ZMQ.Socket killsock  = ctx.socket(ZMQ.SUB);
            killsock.connect(
                    Utils.Constants.FLAKE_KILL_CONTROL_SOCK_PREFIX
                            + fid);
            killsock.subscribe(Utils.Constants.PUB_ALL.getBytes());


            LOGGER.info("Open data channel on: {}", port);
            final ZMQ.Socket backend  = ctx.socket(ZMQ.PUB);
            backend.bind(
                  Utils.Constants.FLAKE_SENDER_BACKEND_SOCK_PREFIX
                    + port);

            LOGGER.info("Open back channel on: {}", backChannelPort);
            final ZMQ.Socket backendBackChannel = ctx.socket(ZMQ.SUB);
            backendBackChannel.bind(
                    Utils.Constants.FLAKE_SENDER_BACKEND_SOCK_PREFIX
                            + backChannelPort);
            backendBackChannel.subscribe("".getBytes());


            ZMQ.Poller pollerItems = new ZMQ.Poller(2 + 1);
            pollerItems.register(middleendreceiver, ZMQ.Poller.POLLIN);
            pollerItems.register(killsock, ZMQ.Poller.POLLIN);
            pollerItems.register(backendBackChannel, ZMQ.Poller.POLLIN);

//            Thread shutdownHook = new Thread(
//                    new Runnable() {
//                        @Override
//                        public void run() {
//                            LOGGER.info("Closing flake killsock.");
//                            middleendreceiver.close();
//                            killsock.close();
//                            backend.close();
//                            backendBackChannel.close();
//                        }
//                    });
//            Runtime.getRuntime().addShutdownHook(shutdownHook);

            int i = 0;
            byte[] message;
            String streamName;

            while (!Thread.currentThread().isInterrupted()) {

                pollerItems.poll();
                if (pollerItems.pollin(0)) { //data messages
                    streamName = middleendreceiver
                            .recvStr(Charset.defaultCharset());
                    message = middleendreceiver.recv();

                    Tuple tuple = tupleSerializer.deserialize(message);

                    List<String> flakeIds = dispersionStrategy
                            .getTargetFlakeIds(tuple);

                    if (flakeIds != null
                            && flakeIds.size() > 0) {
                        for (String flakeId : flakeIds) {
                            LOGGER.debug("Sending to:" + flakeId);
                            backend.sendMore(flakeId);
                            backend.sendMore(myPelletName);
                            backend.send(message, 0);
                        }
                    } else { //should queue up messages.
                        LOGGER.warn("Message dropped because no connection "
                                + "received");
                        //TODO: FIX THIS..
                    }
                } else if (pollerItems.pollin(1)) { //kill signal
                    break;
                } else if (pollerItems.pollin(2)) { //backChannel from successor
                    String flakeId = backendBackChannel.recvStr(
                            Charset.defaultCharset());
                    LOGGER.debug("MSG ON BACKCHANNEL: {}", flakeId);
                    byte[] data = null;
                    if (backendBackChannel.hasReceiveMore()) {
                        data = backendBackChannel.recv();
                    }
                    dispersionStrategy.backChannelMessageReceived(
                            flakeId, data);
                }
            }
            //ZMQ.proxy(middleendreceiver, backend, null);
            LOGGER.warn("Closing flake backend sockets");

            middleendreceiver.close();
            killsock.close();
            backend.close();
            backendBackChannel.close();

            //Runtime.getRuntime().removeShutdownHook(shutdownHook);
        }
    }


    /**
     * The middleend class. A single instance of this is created and is
     * responsible for gathering data from the frontend and sending it to the
     * backend given the dispersion strategy. Currently PUB-SUB with
     * duplicate to all strategy is used.
     */
    private class MiddleEnd extends Thread {
        /**
         * Middleend thread's run method. This is responsible for the routing.
         */
        public void run() {
            final ZMQ.Socket frontend  = ctx.socket(ZMQ.PULL);
            frontend.bind(
                    Utils.Constants.FLAKE_SENDER_FRONTEND_SOCK_PREFIX
                            + fid);


            final ZMQ.Socket middleend  = ctx.socket(ZMQ.PUB);
            middleend.bind(
                    Utils.Constants.FLAKE_SENDER_MIDDLEEND_SOCK_PREFIX
                            + fid);


            final ZMQ.Socket killsock  = ctx.socket(ZMQ.SUB);
            killsock.connect(
                    Utils.Constants.FLAKE_KILL_CONTROL_SOCK_PREFIX
                            + fid);
            killsock.subscribe(Utils.Constants.PUB_ALL.getBytes());

            ZMQ.Poller pollerItems = new ZMQ.Poller(2);
            pollerItems.register(frontend, ZMQ.Poller.POLLIN);
            pollerItems.register(killsock, ZMQ.Poller.POLLIN);

//            Thread shutdownHook = new Thread(
//                    new Runnable() {
//                        @Override
//                        public void run() {
//                            LOGGER.info("Closing flake killsock.");
//                            frontend.close();
//                            middleend.close();
//                            killsock.close();
//                        }
//                    });
//            Runtime.getRuntime().addShutdownHook(shutdownHook);

            while (!Thread.currentThread().isInterrupted()) {
                byte[] message;
                String streamName;
                pollerItems.poll();
                if (pollerItems.pollin(0)) {
                    streamName = frontend.recvStr(Charset.defaultCharset());
                    message = frontend.recv();
                    LOGGER.debug("MD: {}", streamName);
                    LOGGER.debug("MD MS: {}", message);
                    middleend.sendMore(streamName);
                    middleend.send(message, 0);
                } else if (pollerItems.pollin(1)) {
                    break;
                }
            }
            LOGGER.warn("Closing flake middleend sockets");
            frontend.close();
            middleend.close();
            killsock.close();
            //Runtime.getRuntime().removeShutdownHook(shutdownHook);
        }
    }
}
