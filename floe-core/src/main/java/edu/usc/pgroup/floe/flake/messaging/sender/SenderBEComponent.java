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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.flake.FlakeComponent;
import edu.usc.pgroup.floe.flake.messaging
        .dispersion.MessageDispersionStrategy;
import edu.usc.pgroup.floe.flake.messaging
        .dispersion.MessageDispersionStrategyFactory;
import edu.usc.pgroup.floe.serialization.SerializerFactory;
import edu.usc.pgroup.floe.serialization.TupleSerializer;
import edu.usc.pgroup.floe.thriftgen.TChannel;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.nio.charset.Charset;
import java.util.List;

/**
 * @author kumbhare
 */
public class SenderBEComponent extends FlakeComponent {

    /**
     * port on which the back end should listen for
     *             connections from downstream.
     */
    private final int port;


    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SenderBEComponent.class);


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
     * Pellet's name to be sent with each message.
     */
    private final String myPelletName;

    /**
     * Serializer to be used to serialize and deserialize the data tuples.
     */
    private final TupleSerializer tupleSerializer;

    /**
     * Channel type for this backend.
     */
    private final TChannel channelType;

    /**
     * Application name.
     */
    private final String appName;

    /**
     * Dest. pellet name for this BE component.
     */
    private final String destPelletName;


    /**
     * Message dispersion strategy to be used for the channel.
     */
    private MessageDispersionStrategy dispersionStrategy;

    /**
     * Constructor.
     * @param metricRegistry Metrics registry used to log various metrics.
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     * @param p port on which the back end should listen for
     *             connections from downstream.
     * @param bp dispersion port.
     * @param app Application name.
     * @param destPellet dest pellet name to be used to get data from ZK.
     * @param channel channel type (e.g. round robin, reduce,
     *                    load balanced, custom)
     * @param streams list of stream names to subscribe to.
     * @param pelletName Pellet's id/name.
     */
    public SenderBEComponent(final MetricRegistry metricRegistry,
                             final String flakeId,
                             final String componentName,
                             final ZMQ.Context ctx,
                             final int p,
                             final int bp,
                             final String app,
                             final String destPellet,
                             final TChannel channel,
                             final List<String> streams,
                             final String pelletName) {
        super(metricRegistry, flakeId, componentName, ctx);
        this.port = p;
        this.backChannelPort = bp;
        this.streamNames = streams;
        this.myPelletName = pelletName;
        this.tupleSerializer = SerializerFactory.getSerializer();
        this.channelType = channel;
        this.appName = app;
        this.destPelletName = destPellet;
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


        this.dispersionStrategy = null;

        if (channelType != null && channelType.get_channelType() != null) {
            LOGGER.info("type and args: {}, Channel type: {}",
                    channelType.get_channelType(),
                    channelType.get_channelArgs());
            try {
                this.dispersionStrategy = MessageDispersionStrategyFactory
                        .getMessageDispersionStrategy(destPelletName,
                                appName, getFid(), channelType);
            } catch (Exception ex) {
                LOGGER.error("Invalid dispersion strategy: {}. "
                        + "Using default RR", channelType.get_channelType());
            }
        }

        final ZMQ.Socket middleendreceiver  = getContext().socket(ZMQ.SUB);

        middleendreceiver.connect(
                Utils.Constants.FLAKE_SENDER_MIDDLEEND_SOCK_PREFIX + getFid());

        if (streamNames != null) {
            for (String streamName : streamNames) {
                LOGGER.info("Subscribing: {}", streamName);
                middleendreceiver.subscribe(streamName.getBytes());
            }
        }

        LOGGER.info("Open data channel on: {}", port);
        final ZMQ.Socket backend  = getContext().socket(ZMQ.PUB);
        backend.bind(Utils.Constants.FLAKE_SENDER_BACKEND_SOCK_PREFIX
                        + port);

        LOGGER.info("Open back channel on: {}", backChannelPort);
        final ZMQ.Socket backendBackChannel = getContext().socket(ZMQ.REP);
        backendBackChannel.bind(
                Utils.Constants.FLAKE_SENDER_BACKEND_SOCK_PREFIX
                        + backChannelPort);
        //backendBackChannel.subscribe(myPelletName.getBytes());

        ZMQ.Poller pollerItems = new ZMQ.Poller(2 + 1);
        pollerItems.register(middleendreceiver, ZMQ.Poller.POLLIN);
        pollerItems.register(terminateSignalReceiver, ZMQ.Poller.POLLIN);
        //pollerItems.register(backendBackChannel, ZMQ.Poller.POLLIN);

        Meter msgSendMeter =  getMetricRegistry().meter(
                MetricRegistry.name(SenderBEComponent.class, "sent"));

        notifyStarted(true);
        int i = 0;
        byte[] message;
        String streamName;

        /*LOGGER.error("Waiting for backchannel connection at: {} {}",
                myPelletName, backChannelPort);
        backendBackChannel.recv();
        LOGGER.error("Sending ping back:{}", backChannelPort);
        backendBackChannel.send("dne");*/

        while (!Thread.currentThread().isInterrupted()) {
            pollerItems.poll();
            if (pollerItems.pollin(0)) { //data messages
                streamName = middleendreceiver
                        .recvStr(Charset.defaultCharset()); //read an ignore.

                //dispersionStrategy.disperseMessage(middleendreceiver,
                // backend);

                message = middleendreceiver.recv();

                Tuple tuple = tupleSerializer.deserialize(message);

                List<String> flakeIds = dispersionStrategy
                        .getTargetFlakeIds(tuple);

                //FIXME: CAN IMPROVE PERF. HERE BY NOT DOING MULTIPLE
                //FIXME: SERIALIZE/DESERIALIZE operations.
                /*tuple.put(Utils.Constants.SYSTEM_TS_FIELD_NAME,
                        System.nanoTime());
                tuple.put(Utils.Constants.SYSTEM_SRC_PELLET_NAME,
                        myPelletName);
                message = tupleSerializer.serialize(tuple);*/
                if (flakeIds != null
                        && flakeIds.size() > 0) {
                    for (String flakeId : flakeIds) {
                        LOGGER.debug("Sending {} to: {}", tuple, flakeId);
                        backend.sendMore(flakeId);
                        backend.sendMore(myPelletName);

                        List<String> fargs = dispersionStrategy
                                .getCustomArguments(flakeId);

                        if (fargs != null) {
                            backend.sendMore(String.valueOf(fargs.size()));
                            for (String arg: fargs) {
                                backend.sendMore(arg);
                            }
                        } else {
                            backend.sendMore(String.valueOf(0));
                        }

                        backend.send(message, 0);
                    }
                } else { //should queue up messages.
                    LOGGER.error("Message dropped because no connection "
                            + "received");    //TODO: FIX THIS..
                }
                msgSendMeter.mark();
            } else if (pollerItems.pollin(1)) { //kill signal
                LOGGER.warn("Terminating flake sender ME: {}", getFid());
                terminateSignalReceiver.recv();
                break;
            } /*else if (pollerItems.pollin(2)) { //backChannel from successor
                String mypid = backendBackChannel.recvStr(
                        Charset.defaultCharset());
                String flakeId = backendBackChannel.recvStr(
                        Charset.defaultCharset());
                String toContinue = backendBackChannel.recvStr(
                        Charset.defaultCharset());
                byte[] data = null;
                if (backendBackChannel.hasReceiveMore()) {
                    data = backendBackChannel.recv();
                }
                if (data != null) {
                    LOGGER.debug("MSG ON BACKCHANNEL: {},{}",
                            data, toContinue);
                } else {
                    LOGGER.debug("MSG ON BACKCHANNEL: {},{}",
                            "NNOOOOO DATA", toContinue);
                }
                Boolean btoContinue = true;
                if ("0".equalsIgnoreCase(toContinue)) {
                    btoContinue = false;
                    LOGGER.warn("TERMINATE MSG ON BACKCHANNEL: {}",
                            btoContinue);
                }
                //NOTE: WE ARE NOT USING BACKCHANNEL ANY MORE!!! CLEANUP REST
                // OF THE CODE AS WELL.
                /*dispersionStrategy.backChannelMessageReceived(
                        flakeId, data, btoContinue);*
            }*/
        }

        LOGGER.warn("Closing flake backend sockets");

        middleendreceiver.close();
        backend.close();
        backendBackChannel.close();
        notifyStopped(true);
    }
}
