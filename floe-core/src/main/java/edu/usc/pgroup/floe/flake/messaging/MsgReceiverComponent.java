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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.container.FlakeControlCommand;
import edu.usc.pgroup.floe.flake.FlakeComponent;
import edu.usc.pgroup.floe.flake.QueueLenMonitor;
import edu.usc.pgroup.floe.flake.messaging
        .dispersion.FlakeLocalDispersionStrategy;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author kumbhare
 */
public class MsgReceiverComponent extends FlakeComponent {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(MsgReceiverComponent.class);

    /**
     * Flake's token on the ring.
     */
    private final Integer myToken;


    /**
     * Predecessor to channel type map.
     */
    private Map<String, String> predChannelMap;

    /**
     * The neighbors currently subscribed for.
     */
    private List<String> neighborsSubscribedFor;

    /**
     * Receiver ME component.
     */
    private ReceiverME receiverMEComponent;


    /**
     * Constructor.
     * @param metricRegistry Metrics registry used to log various metrics.
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     * @param predChannelTypeMap the pred. to channel type map.
     * @param token Flake's token on the ring.
     */
    public MsgReceiverComponent(final MetricRegistry metricRegistry,
                                final String flakeId,
                                final String componentName,
                                final ZMQ.Context ctx,
                                final Map<String, String> predChannelTypeMap,
                                final Integer token) {
        super(metricRegistry, flakeId, componentName, ctx);
        this.predChannelMap = predChannelTypeMap;
        this.myToken = token;
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

        //front end and backend for data messages.
        final ZMQ.Socket frontend = getContext().socket(ZMQ.SUB);

        //middle end for the tuples.
        final ZMQ.Socket recevierME = getContext().socket(ZMQ.PUSH);
        recevierME.connect(Utils.Constants.FLAKE_RECEIVER_MIDDLE_PREFIX
                            + getFid());

        //xpub, xsub for backchannels (per edge).
        //final ZMQ.Socket xpubToPredSock = getContext().socket(ZMQ.XPUB);
        //final ZMQ.Socket xsubFromPelletsSock = getContext().socket(ZMQ.XSUB);

        final ZMQ.Socket msgReceivercontrolForwardSocket
                = getContext().socket(ZMQ.REP);

        //back channel pinger to initiate out of bound backchannel message
        // when a new pred. flake is created.
        //final ZMQ.Socket backChannelPingger = getContext().socket(ZMQ.PUB);



        boolean result = false;

        Meter msgRecvMeter =  getMetricRegistry().meter(
                MetricRegistry.name(MsgReceiverComponent.class, "received")
        );

        try {
            //Frontend socket to talk to other flakes. dont connect here.
            // Connect only when the signal for connect is received.
            LOGGER.info("Starting front end receiver socket");
            LOGGER.info("FE subscribing for: {}", getFid());
            frontend.subscribe(getFid().getBytes());

            //SUBSCRIBE HERE FOR THE FLAKES FOR WHICH THIS GUY WILL BE THE
            // BACKUP.






            //XPUB XSUB sockets for the backchannels.
            //LOGGER.info("WAITING FOR BACKCHANNEL CONNECTINON "
            //        + "FROM back channel sender. {}", getFid());
            //xsubFromPelletsSock.bind(
            //       Utils.Constants.FLAKE_BACKCHANNEL_SENDER_PREFIX
            //               + getFid());

            //connect to the predecessor's back channel on connect signal.

            //Start the backchannelsender.
            //backChannelPingger.bind(
            //        Utils.Constants.FLAKE_BACKCHANNEL_CONTROL_PREFIX
            //                + getFid());

            LOGGER.info("Starting backend ipc socket for control channel at: "
                    + Utils.Constants.FLAKE_RECEIVER_CONTROL_FWD_PREFIX
                    + getFid());
            msgReceivercontrolForwardSocket.bind(
                    Utils.Constants.FLAKE_RECEIVER_CONTROL_FWD_PREFIX
                            + getFid());


            receiverMEComponent = new ReceiverME(getMetricRegistry(),
                    getFid(),
                    "RECEIVER-NE", getContext(),
                    predChannelMap,
                    myToken);

            receiverMEComponent.startAndWait();

            result = true;
        } catch (Exception ex) {
            LOGGER.warn("Exception while starting flake {}", ex);
            result = false;
        } finally {
            notifyStarted(result);
        }




        receiveAndProcess(
                msgRecvMeter,
                frontend,
                recevierME,
                //xsubFromPelletsSock,
                //xpubToPredSock,
                msgReceivercontrolForwardSocket,
                //backChannelPingger,
                terminateSignalReceiver
        );

        frontend.close();
        recevierME.close();
        //xpubToPredSock.close();
        //xsubFromPelletsSock.close();
        msgReceivercontrolForwardSocket.close();
        //backChannelPingger.close();

        notifyStopped(result);
    }

    /**
     * Receives and forwards/routes the incoming messages.
     * @param msgRecvMeter Meter to measure the rate of incoming messages.
     * @param frontend The frontend socket to receive all messagess from all
     *                 pred. flakes.
     * @param recevierME socket to forward messages to middleend.
     * @aram xsubFromPelletsSock a raw xsub socket to to forward messages
     *                            from backchannel (per edge) to the pred.
     *                            flakes.
     * @aram xpubToPredSock a raw xpub component for forwarding backchannel
     *                       messages.
     * @param msgReceivercontrolForwardSocket to receive control signals from
     *                                        the flake.
     * @aram backChannelPingger socket to ping the backchannel whenever a
     *                           new pred. flake is added/removed.
     * @param terminateSignalReceiver terminate signal receiver.
     */
    private void receiveAndProcess(
            final Meter msgRecvMeter, final ZMQ.Socket frontend,
            final ZMQ.Socket recevierME,
            //final ZMQ.Socket xsubFromPelletsSock,
            //final ZMQ.Socket xpubToPredSock,
            final ZMQ.Socket msgReceivercontrolForwardSocket,
            //final ZMQ.Socket backChannelPingger,
            final ZMQ.Socket terminateSignalReceiver) {

        byte[] message;

        //Connect to the message backup socket.
        LOGGER.info("FE connecting for: {}",
                Utils.Constants.FLAKE_MSG_RECOVERY_PREFIX + getFid());
        frontend.connect(Utils.Constants.FLAKE_MSG_RECOVERY_PREFIX + getFid());

        ZMQ.Poller pollerItems = new ZMQ.Poller(3);
        pollerItems.register(frontend, ZMQ.Poller.POLLIN);
        //pollerItems.register(backend, ZMQ.Poller.POLLIN);
        //pollerItems.register(xsubFromPelletsSock, ZMQ.Poller.POLLIN);
        //pollerItems.register(xpubToPredSock, ZMQ.Poller.POLLIN);
        pollerItems.register(msgReceivercontrolForwardSocket
                                                , ZMQ.Poller.POLLIN);
        pollerItems.register(terminateSignalReceiver, ZMQ.Poller.POLLIN);

        boolean done = false;
        boolean terminateSignalled = false;
        final int pollDelay = 500;

        Counter queLen = getMetricRegistry().counter(
                MetricRegistry.name(MsgReceiverComponent.class, "queue.len"));

        QueueLenMonitor monitor = new QueueLenMonitor(getMetricRegistry(),
                queLen);
        monitor.start();

        while (!done && !Thread.currentThread().isInterrupted()) {
            pollerItems.poll(pollDelay);

            if (pollerItems.pollin(0)) { //frontend
                //forwardToPellet(msgRecvMeter, frontend, backend,
                //        msgBackupSender);

                msgRecvMeter.mark();
                queLen.inc();
                Utils.forwardCompleteMessage(frontend, recevierME);

            /*} else if (pollerItems.pollin(1)) { //backend
                Utils.forwardCompleteMessage(backend, frontend);
            } else if (pollerItems.pollin(2)) { //from xsubFromPelletsSock
                Utils.forwardCompleteMessage(
                        xsubFromPelletsSock, xpubToPredSock);
            } else if (pollerItems.pollin(3)) { //from xpubToPredSock
                Utils.forwardCompleteMessage(
                        xpubToPredSock, xsubFromPelletsSock);*/
            } else if (pollerItems.pollin(1)) { //controlSocket
                LOGGER.info("Control msg");
                message = msgReceivercontrolForwardSocket.recv();

                byte[] result = new byte[]{1};

                //process control message.
                FlakeControlCommand command
                        = (FlakeControlCommand) Utils.deserialize(
                        message);

                LOGGER.info("Received command: " + command);
                switch (command.getCommand()) {
                    case CONNECT_PRED:
                        String connectstr = (String) command.getData();
                        String dataChannel = connectstr.split(";")[0];
                        String backChannel = connectstr.split(";")[1];

                        LOGGER.info("data channel: " + dataChannel);
                        LOGGER.info("back channel: " + backChannel);
                        LOGGER.info("FE connecting for: {}",
                                dataChannel);
                        frontend.connect(dataChannel);
                        //xpubToPredSock.connect(backChannel);
                        result[0] = 1;
                        //backChannelPingger.send(result, 0);
                        break;
                    case DISCONNECT_PRED:
                        String disconnectstr = (String) command.getData();
                        LOGGER.info("disconnecting from: " + disconnectstr);
                        frontend.disconnect(disconnectstr);
                        break;
                    case INCREMENT_PELLET:
                        String peId = (String) command.getData();
                        notifyPelletAdded(peId);
                        break;
                    case DECREMENT_PELLET:
                        String dpeId = (String) command.getData();
                        notifyPelletRemoved(dpeId);
                        break;
                    case UPDATE_SUBSCRIPTION:
                        List<String> currentNeighborsToSubscribe
                                = (List<String>) command.getData();
                        updateFrontendSubscription(
                                frontend, currentNeighborsToSubscribe);
                        break;
                    default:
                        LOGGER.warn("Should have been processed by the flake.");
                }
                msgReceivercontrolForwardSocket.send(result, 0);
            } else if (pollerItems.pollin(2)) { //interrupt socket
                //HOW DO WE PROCESS PENDING MESSAGES? OR DO WE NEED TO?
                byte[] intr = terminateSignalReceiver.recv();
                terminateSignalled = true;
            } else {
                if (terminateSignalled) {
                    done = true;
                }
            }
        }
        monitor.interrupt();
    }

    /**
     * Updates the front end subscription.
     * @param frontend the frontend socket to update the subscriptions for.
     * @param newNeighborsToSubscribe the list of neighbors to subscribe
     *                                    for received from the coordination
     */
    private void updateFrontendSubscription(
            final ZMQ.Socket frontend,
            final List<String> newNeighborsToSubscribe) {

        List<String> toAdd = new ArrayList<>();
        List<String> toRemove = new ArrayList<>();


        LOGGER.info("UPDATING frontend subscriptions.");

        if (this.neighborsSubscribedFor == null) {
            this.neighborsSubscribedFor = newNeighborsToSubscribe;
            toAdd.addAll(this.neighborsSubscribedFor);
        } else {

            for (String currentSubscribed: neighborsSubscribedFor) {
                if (!newNeighborsToSubscribe.contains(currentSubscribed)) {
                    toRemove.add(currentSubscribed);
                }
            }

            for (String newNeighbor: newNeighborsToSubscribe) {
                if (!neighborsSubscribedFor.contains(newNeighbor)) {
                    toAdd.add(newNeighbor);
                }
            }
        }

        for (String nfid: toAdd) {
            LOGGER.info("ME:{} subscribing for:{}.", getFid(), nfid);
            LOGGER.info("FE subscribing for: {}", nfid);
            frontend.subscribe(nfid.getBytes());
        }

        for (String nfid: toRemove) {
            LOGGER.info("ME:{} UNsubscribing for:{}.", getFid(), nfid);
            frontend.unsubscribe(nfid.getBytes());
        }

        neighborsSubscribedFor.removeAll(toRemove);
        neighborsSubscribedFor.addAll(toAdd);
    }


    /**
     * NOtifies all strategy instances that a pellet has been added.
     * @param peInstanceId instance id for the added pellet.
     */
    private void notifyPelletAdded(
            final String peInstanceId) {
        Map<String, FlakeLocalDispersionStrategy> localDispersionStratMap
                = receiverMEComponent.getStratMap();
        for (FlakeLocalDispersionStrategy strat
                : localDispersionStratMap.values()) {
            strat.pelletAdded(peInstanceId);
        }
    }

    /**
     * NOtifies all strategy instances that a pellet has been removed.
     * @param peInstanceId instance id for the added pellet.
     */
    private void notifyPelletRemoved(
            final String peInstanceId) {
        Map<String, FlakeLocalDispersionStrategy> localDispersionStratMap
                = receiverMEComponent.getStratMap();
        for (FlakeLocalDispersionStrategy strat
                : localDispersionStratMap.values()) {
            strat.pelletRemoved(peInstanceId);
        }
    }
}
