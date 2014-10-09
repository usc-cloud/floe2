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
import edu.usc.pgroup.floe.container.FlakeControlCommand;
import edu.usc.pgroup.floe.flake.FlakeComponent;
import edu.usc.pgroup.floe.flake.messaging
        .dispersion.FlakeLocalDispersionStrategy;
import edu.usc.pgroup.floe.flake.messaging
        .dispersion.MessageDispersionStrategyFactory;
import edu.usc.pgroup.floe.serialization.SerializerFactory;
import edu.usc.pgroup.floe.serialization.TupleSerializer;
import edu.usc.pgroup.floe.thriftgen.TChannelType;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
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
     * Map of pred. pellet name to local dispersion strategy.
     */
    private final
    Map<String, FlakeLocalDispersionStrategy> localDispersionStratMap;

    /**
     * Serializer to be used to serialize and deserialized the data tuples.
     */
    private final TupleSerializer tupleSerializer;

    /**
     * The neighbors currently subscribed for.
     */
    private List<String> neighborsSubscribedFor;

    /**
     * Constructor.
     *
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     * @param predChannelTypeMap the pred. to channel type map.
     * @param token Flake's token on the ring.
     */
    public MsgReceiverComponent(final String flakeId,
                                final String componentName,
                                final ZMQ.Context ctx,
                                final Map<String, String> predChannelTypeMap,
                                final Integer token) {
        super(flakeId, componentName, ctx);
        this.predChannelMap = predChannelTypeMap;
        this.localDispersionStratMap = new HashMap<>();
        this.tupleSerializer = SerializerFactory.getSerializer();
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
        final ZMQ.Socket backend = getContext().socket(ZMQ.PUB);

        //xpub, xsub for backchannels (per edge).
        final ZMQ.Socket xpubToPredSock = getContext().socket(ZMQ.XPUB);
        final ZMQ.Socket xsubFromPelletsSock = getContext().socket(ZMQ.XSUB);

        final ZMQ.Socket msgReceivercontrolForwardSocket
                = getContext().socket(ZMQ.REP);

        //back channel pinger to initiate out of bound backchannel message
        // when a new pred. flake is created.
        final ZMQ.Socket backChannelPingger = getContext().socket(ZMQ.PUB);

        final ZMQ.Socket msgBackupSender = getContext().socket(ZMQ.PUSH);

        boolean result = false;
        try {
            //Frontend socket to talk to other flakes. dont connect here.
            // Connect only when the signal for connect is received.
            LOGGER.info("Starting front end receiver socket");
            frontend.subscribe(getFid().getBytes());

            //SUBSCRIBE HERE FOR THE FLAKES FOR WHICH THIS GUY WILL BE THE
            // BACKUP.



            //Backend socket to talk to the Pellets contained in the flake. The
            // pellets may be added or removed dynamically.
            LOGGER.info("Starting backend inproc socket to communicate with "
                    + "pellets at: "
                    + Utils.Constants.FLAKE_RECEIVER_BACKEND_SOCK_PREFIX
                    + getFid());
            backend.bind(Utils.Constants.FLAKE_RECEIVER_BACKEND_SOCK_PREFIX
                    + getFid());



            //XPUB XSUB sockets for the backchannels.
            LOGGER.info("WAITING FOR BACKCHANNEL CONNECTINON "
                    + "FROM back channel sender. {}", getFid());
            xsubFromPelletsSock.bind(
                    Utils.Constants.FLAKE_BACKCHANNEL_SENDER_PREFIX
                            + getFid());

            //connect to the predecessor's back channel on connect signal.

            //Start the backchannelsender.
            backChannelPingger.bind(
                    Utils.Constants.FLAKE_BACKCHANNEL_CONTROL_PREFIX
                            + getFid());

            LOGGER.info("Starting backend ipc socket for control channel at: "
                    + Utils.Constants.FLAKE_RECEIVER_CONTROL_FWD_PREFIX
                    + getFid());
            msgReceivercontrolForwardSocket.bind(
                    Utils.Constants.FLAKE_RECEIVER_CONTROL_FWD_PREFIX
                            + getFid());


            //initialize per edge flake local strategies.
            initializeLocalDispersionStrategyMap();

            msgBackupSender.connect(Utils.Constants.FLAKE_MSG_BACKUP_PREFIX
                    + getFid());

            result = true;
        } catch (Exception ex) {
            LOGGER.warn("Exception while starting flake {}", ex);
            result = false;
        } finally {
            notifyStarted(result);
        }

        receiveAndProcess(
                frontend,
                backend,
                xsubFromPelletsSock,
                xpubToPredSock,
                msgReceivercontrolForwardSocket,
                backChannelPingger,
                terminateSignalReceiver,
                msgBackupSender
        );

        frontend.close();
        backend.close();
        xpubToPredSock.close();
        xsubFromPelletsSock.close();
        msgReceivercontrolForwardSocket.close();
        backChannelPingger.close();

        notifyStopped(result);
    }

    /**
     * Receives and forwards/routes the incoming messages.
     * @param frontend The frontend socket to receive all messagess from all
     *                 pred. flakes.
     * @param backend backend socket to forward messages to appropriate
     *                pellet instances.
     * @param xsubFromPelletsSock a raw xsub socket to to forward messages
     *                            from backchannel (per edge) to the pred.
     *                            flakes.
     * @param xpubToPredSock a raw xpub component for forwarding backchannel
     *                       messages.
     * @param msgReceivercontrolForwardSocket to receive control signals from
     *                                        the flake.
     * @param backChannelPingger socket to ping the backchannel whenever a
     *                           new pred. flake is added/removed.
     * @param terminateSignalReceiver terminate signal receiver.
     * @param msgBackupSender socket to send the tuples meant for backup .
     */
    private void receiveAndProcess(
            final ZMQ.Socket frontend,
            final ZMQ.Socket backend,
            final ZMQ.Socket xsubFromPelletsSock,
            final ZMQ.Socket xpubToPredSock,
            final ZMQ.Socket msgReceivercontrolForwardSocket,
            final ZMQ.Socket backChannelPingger,
            final ZMQ.Socket terminateSignalReceiver,
            final ZMQ.Socket msgBackupSender) {

        byte[] message;

        //Connect to the message backup socket.
        frontend.connect(Utils.Constants.FLAKE_MSG_RECOVERY_PREFIX + getFid());

        ZMQ.Poller pollerItems = new ZMQ.Poller(6);
        pollerItems.register(frontend, ZMQ.Poller.POLLIN);
        pollerItems.register(backend, ZMQ.Poller.POLLIN);
        pollerItems.register(xsubFromPelletsSock, ZMQ.Poller.POLLIN);
        pollerItems.register(xpubToPredSock, ZMQ.Poller.POLLIN);
        pollerItems.register(msgReceivercontrolForwardSocket
                                                , ZMQ.Poller.POLLIN);
        pollerItems.register(terminateSignalReceiver, ZMQ.Poller.POLLIN);

        boolean done = false;
        boolean terminateSignalled = false;
        final int pollDelay = 500;
        while (!done && !Thread.currentThread().isInterrupted()) {
            pollerItems.poll(pollDelay);

            if (pollerItems.pollin(0)) { //frontend
                forwardToPellet(frontend, backend, msgBackupSender);
            } else if (pollerItems.pollin(1)) { //backend
                Utils.forwardCompleteMessage(backend, frontend);
            } else if (pollerItems.pollin(2)) { //from xsubFromPelletsSock
                Utils.forwardCompleteMessage(
                        xsubFromPelletsSock, xpubToPredSock);
            } else if (pollerItems.pollin(3)) { //from xpubToPredSock
                Utils.forwardCompleteMessage(
                        xpubToPredSock, xsubFromPelletsSock);
            } else if (pollerItems.pollin(4)) { //controlSocket
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
                        frontend.connect(dataChannel);
                        xpubToPredSock.connect(backChannel);
                        result[0] = 1;
                        backChannelPingger.send(result, 0);
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
            } else if (pollerItems.pollin(5)) { //interrupt socket
                //HOW DO WE PROCESS PENDING MESSAGES? OR DO WE NEED TO?
                for (FlakeLocalDispersionStrategy strategy
                        : localDispersionStratMap.values()) {
                    strategy.stopAndWait();
                }
                byte[] intr = terminateSignalReceiver.recv();
                terminateSignalled = true;
            } else {
                if (terminateSignalled) {
                    done = true;
                }
            }
        }
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
     * Once the poller.poll returns, use this function as a component in the
     * proxy to forward messages from one socket to another.
     * @param from socket to read from.
     * @param to socket to send messages to.
     * @param backup socket to send to backup the messages. Using socket and
     *               not backing up directly to minimize latencies.
     */
    private void forwardToPellet(final ZMQ.Socket from,
                                 final ZMQ.Socket to,
                                 final ZMQ.Socket backup) {
        String fid = from.recvStr(0, Charset.defaultCharset());

        byte[] message = from.recv();

        Tuple t = tupleSerializer.deserialize(message);

        if (!fid.equalsIgnoreCase(getFid())) {
            LOGGER.info("THIS MESSAGE IS MEANT FOR BACKUP."
                    + " SHOULD DO THAT HERE {} & {}", fid, getFid());
            backup.sendMore(fid);
            backup.send(message, 0);
            return;
        }

        String src = (String) t.get(Utils.Constants.SYSTEM_SRC_PELLET_NAME);

        FlakeLocalDispersionStrategy strategy
                = localDispersionStratMap.get(src);

        if (strategy == null) {
            LOGGER.info("No strategy found. Dropping message.");
            return;
        }

        List<String> pelletInstancesIds =
                strategy.getTargetPelletInstances(t);

        if (pelletInstancesIds != null
                && pelletInstancesIds.size() > 0) {
            for (String pelletInstanceId : pelletInstancesIds) {
                LOGGER.debug("Sending to:" + pelletInstanceId);
                to.sendMore(pelletInstanceId);
                to.send(message, 0);
            }
        } else { //should queue up messages.
            LOGGER.warn("Message dropped because no pellet active.");
            //TODO: FIX THIS..
        }

        LOGGER.debug("Received msg from:" + src);
    }

    /**
     * Initializes the pred. strategy map.
     */
    private void initializeLocalDispersionStrategyMap() {

        for (Map.Entry<String, String> channel: predChannelMap.entrySet()) {
            String src = channel.getKey();
            String channelType = channel.getValue();

            String[] ctypesAndArgs = channelType.split("__");
            String ctype = ctypesAndArgs[0];
            String args = null;
            if (ctypesAndArgs.length > 1) {
                args = ctypesAndArgs[1];
            }
            LOGGER.info("type and args: {}, Channel type: {}", channelType,
                    ctype);

            FlakeLocalDispersionStrategy strat = null;

            if (!ctype.startsWith("NONE")) {
                TChannelType type = Enum.valueOf(TChannelType.class, ctype);
                try {
                    strat = MessageDispersionStrategyFactory
                                .getFlakeLocalDispersionStrategy(
                                    type,
                                    src,
                                    getContext(),
                                    getFid(),
                                    myToken,
                                    args
                                );
                    strat.startAndWait();
                    localDispersionStratMap.put(src, strat);

                    //forward the first back message before this returns..
                    // should help. lets see.

                } catch (Exception ex) {
                    LOGGER.error("Invalid dispersion strategy: {}. "
                            + "Using default RR", type);
                }
            }
        }
    }

    /**
     * NOtifies all strategy instances that a pellet has been added.
     * @param peInstanceId instance id for the added pellet.
     */
    private void notifyPelletAdded(
            final String peInstanceId) {
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
        for (FlakeLocalDispersionStrategy strat
                : localDispersionStratMap.values()) {
            strat.pelletRemoved(peInstanceId);
        }
    }
}
