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

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import edu.usc.pgroup.floe.app.Tuple;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author kumbhare
 */
public class ReceiverME extends FlakeComponent {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ReceiverME.class);

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
     * Timer to measure approx. nw. latency.
     */
    private final Timer nwLatTimer;

    /**
     * Constructor.
     *
     * @param registry      Metrics registry used to log various metrics.
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     * @param predChannelTypeMap the pred. to channel type map.
     * @param token Flake's token on the ring.
     */
    public ReceiverME(final MetricRegistry registry,
                      final String flakeId,
                      final String componentName,
                      final ZMQ.Context ctx,
                      final Map<String, String> predChannelTypeMap,
                      final Integer token) {
        super(registry, flakeId, componentName, ctx);
        this.myToken = token;
        this.predChannelMap = predChannelTypeMap;
        this.localDispersionStratMap = new HashMap<>();
        this.tupleSerializer = SerializerFactory.getSerializer();
        nwLatTimer = registry.timer(
                MetricRegistry.name(MsgReceiverComponent.class, "nw.latency")
        );
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

        LOGGER.info("Starting backend inproc socket to communicate with "
                + "pellets at: "
                + Utils.Constants.FLAKE_RECEIVER_BACKEND_SOCK_PREFIX
                + getFid());
        final ZMQ.Socket backend = getContext().socket(ZMQ.PUB);
        backend.bind(Utils.Constants.FLAKE_RECEIVER_BACKEND_SOCK_PREFIX
                + getFid());


        final ZMQ.Socket msgBackupSender = getContext().socket(ZMQ.PUSH);
        msgBackupSender.connect(Utils.Constants.FLAKE_MSG_BACKUP_PREFIX
                + getFid());

        final ZMQ.Socket recevierME = getContext().socket(ZMQ.PULL);
        recevierME.bind(Utils.Constants.FLAKE_RECEIVER_MIDDLE_PREFIX
                + getFid());

        initializeLocalDispersionStrategyMap();


        notifyStarted(true);

        ZMQ.Poller pollerItems = new ZMQ.Poller(2);
        pollerItems.register(recevierME, ZMQ.Poller.POLLIN);
        pollerItems.register(terminateSignalReceiver, ZMQ.Poller.POLLIN);


        final int pollDelay = 500;
        while (!Thread.currentThread().isInterrupted()) {
            pollerItems.poll(pollDelay);

            if (pollerItems.pollin(0)) { //forward to pellet
                forwardToPellet(recevierME, backend, msgBackupSender);
            } else if (pollerItems.pollin(1)) { //interrupt socket
                //HOW DO WE PROCESS PENDING MESSAGES? OR DO WE NEED TO?
                for (FlakeLocalDispersionStrategy strategy
                        : localDispersionStratMap.values()) {
                    strategy.stopAndWait();
                }
                byte[] intr = terminateSignalReceiver.recv();
                break;
            }
        }

        backend.close();
        recevierME.close();
        msgBackupSender.close();
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
                                    getMetricRegistry(),
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
     * Once the poller.poll returns, use this function as a component in the
     * proxy to forward messages from one socket to another.
     * @param from socket to read from.
     * @param to socket to send messages to.
     * @param backup socket to send to backup the messages. Using socket and
     */
    private void forwardToPellet(final ZMQ.Socket from,
                                 final ZMQ.Socket to,
                                 final ZMQ.Socket backup) {
        String fid = from.recvStr(0, Charset.defaultCharset());

        byte[] message = from.recv();

        /*int dummy = 0;
        LOGGER.info("dummy:{}", dummy);
        if (dummy == 0) {
            LOGGER.info("returning");
            return;
        }*/

        LOGGER.info("has more:{}", from.hasReceiveMore());

        Long currentNano = System.nanoTime();

        Tuple t = tupleSerializer.deserialize(message);

        Long ts = (Long) t.get(Utils.Constants.SYSTEM_TS_FIELD_NAME);

        Long approxNwLat  = currentNano - ts;
        if (approxNwLat > 0) {
            nwLatTimer.update(approxNwLat, TimeUnit.NANOSECONDS);
        }

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

        LOGGER.debug("Forwarding to pellet: {}", t);
        List<String> pelletInstancesIds =
                strategy.getTargetPelletInstances(t);

        if (pelletInstancesIds != null
                && pelletInstancesIds.size() > 0) {
            for (String pelletInstanceId : pelletInstancesIds) {
                LOGGER.debug("Sending to:" + pelletInstanceId);
                to.sendMore(pelletInstanceId);
                to.sendMore(currentNano.toString());
                to.send(message, 0);
            }
        } else { //should queue up messages.
            LOGGER.warn("Message dropped because no pellet active.");
            //FIXME: FIX THIS..
        }

        LOGGER.debug("Received msg from:" + src);
    }

    /**
     * @return the local strat. map from "
     */
    public final Map<String, FlakeLocalDispersionStrategy> getStratMap() {
        return localDispersionStratMap;
    }
}