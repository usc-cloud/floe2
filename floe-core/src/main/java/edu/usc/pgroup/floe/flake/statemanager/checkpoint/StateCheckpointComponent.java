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

package edu.usc.pgroup.floe.flake.statemanager.checkpoint;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Snapshot;
import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.flake.FlakeComponent;
import edu.usc.pgroup.floe.flake.FlakeToken;
import edu.usc.pgroup.floe.flake.PelletExecutor;
import edu.usc.pgroup.floe.flake.QueueLenMonitor;
import edu.usc.pgroup.floe.flake.coordination.PeerMonitor;
import edu.usc.pgroup.floe.flake.coordination.PeerUpdateListener;
import edu.usc.pgroup.floe.flake.messaging.MsgReceiverComponent;
import edu.usc.pgroup.floe.flake.statemanager.StateManager;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * @author kumbhare
 */
public class StateCheckpointComponent extends FlakeComponent
        implements PeerUpdateListener {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(StateCheckpointComponent.class);

    /**
     * CHeckpointing period.
     */
    private final int checkpointPeriod;

    /**
     * State manager instance.
     */
    private final StateManager stateManager;

    /**
     * Peer monitor.
     */
    private final PeerMonitor peerMonitor;

    /**
     * State socket listener.
     */
    private ZMQ.Socket stateSocReceiver;

    /**
     * Port to bind the socket to send periodic state checkpoints.
     */
    private int port;

    /**
     * Constructor.
     * @param metricRegistry Metrics registry used to log various metrics.
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     * @param stateMgr State manager component.
     * @param stateChkptPort port to use for connections to checkpoint state.
     * @param monitor peer monitor.
     */
    public StateCheckpointComponent(final MetricRegistry metricRegistry,
                                    final String flakeId,
                                    final String componentName,
                                    final ZMQ.Context ctx,
                                    final StateManager stateMgr,
                                    final int stateChkptPort,
                                    final PeerMonitor monitor) {
        super(metricRegistry, flakeId, componentName, ctx);
        this.stateManager = stateMgr;
        this.port = stateChkptPort;
        this.checkpointPeriod = FloeConfig.getConfig().getInt(ConfigProperties
                .FLAKE_STATE_CHECKPOINT_PERIOD) * Utils.Constants.MILLI;
        this.stateSocReceiver = getContext().socket(ZMQ.PULL);
        this.peerMonitor = monitor;
        this.peerMonitor.addPeerUpdateListener(this);
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

        ZMQ.Poller pollerItems = new ZMQ.Poller(1);
        pollerItems.register(terminateSignalReceiver, ZMQ.Poller.POLLIN);
        pollerItems.register(stateSocReceiver, ZMQ.Poller.POLLIN);

        /**
         * ZMQ socket connection publish the state to the backups.
         */
        ZMQ.Socket stateSocSender = getContext().socket(ZMQ.PUSH);
        String ssConnetStr = Utils.Constants.FLAKE_STATE_PUB_SOCK + port;
        LOGGER.info("binding STATE CHECKPOINTER to socket at: {}", ssConnetStr);

        TimeUnit durationUnit = TimeUnit.MILLISECONDS;
        double durationFactor = 1.0 / durationUnit.toNanos(1);

        stateSocSender.bind(ssConnetStr);
        /*Meter qhist
                = getMetricRegistry().meter(
                MetricRegistry.name(QueueLenMonitor.class, "q.len.histo"));*/

        LOGGER.error("Hists: {}", getMetricRegistry().getHistograms());
        /*Histogram qhist = getMetricRegistry()
                .getHistograms()
                .get(MetricRegistry.name(
                        QueueLenMonitor.class, "q.len.histo"));*/

        Counter queLen = getMetricRegistry().counter(
                MetricRegistry.name(MsgReceiverComponent.class, "queue.len"));

        final int windowlen = 30; //seconds.
        Histogram qhist
                = getMetricRegistry() .register(
                MetricRegistry.name(QueueLenMonitor.class, "q.len.histo"),
                new Histogram(new SlidingTimeWindowReservoir(windowlen,
                        TimeUnit.SECONDS)));

        QueueLenMonitor monitor = new QueueLenMonitor(getMetricRegistry(),
                queLen, qhist);
        monitor.start();



        Meter msgProcessedMeter =  getMetricRegistry().meter(
                MetricRegistry.name(PelletExecutor.class, "processed"));

        notifyStarted(true);
        Boolean done = false;

        long starttime = System.currentTimeMillis();

        final int qLenThreshold = 10;

        notifyStarted(true);


        while (!done && !Thread.currentThread().isInterrupted()) {

            int polled = pollerItems.poll(checkpointPeriod);
            if (pollerItems.pollin(0)) {
                //terminate.
                LOGGER.warn("Terminating state checkpointing");
                terminateSignalReceiver.recv();
                done = true;
            } else if (pollerItems.pollin(1)) {
                String nfid = stateSocReceiver.recvStr(
                                                Charset.defaultCharset());
                String last = stateSocReceiver.recvStr(
                                                Charset.defaultCharset());
                String lb = stateSocReceiver.recvStr(
                                                Charset.defaultCharset());

                Boolean scalingDown = Boolean.parseBoolean(last);
                Boolean loadbalanceReq = Boolean.parseBoolean(lb);

                LOGGER.info("State delta received from:{}", nfid);
                byte[] serializedState = stateSocReceiver.recv();

                stateManager.storeNeighborCheckpointToBackup(nfid,
                        serializedState);

                /*if (scalingDown) {
                    LOGGER.info("Scaling down NEIGHBOR flake: {}", nfid);
                    initiateScaleDownAndTakeOver(nfid, false);
                } else if (loadbalanceReq) {
                    LOGGER.error("LB request received from:{}.", nfid);
                    initiateScaleDownAndTakeOver(nfid, true);
                }

                nowRecovering = false;*/
            } else {

                Snapshot snp = qhist.getSnapshot();
                //snp.dump(System.out);
                LOGGER.info("fid:{}; q 95->{}; 75->{}; 99->{}; msgs procd: {}",
                        getFid(),
                        snp.get95thPercentile(), //* durationFactor,
                        snp.get75thPercentile(), //* durationFactor,
                        snp.get99thPercentile(), //* durationFactor,
                        msgProcessedMeter.getOneMinuteRate());

                //double last1min = qhist.;
            /*LOGGER.error("fid:{}; q 1min->{}; msgs procd: {}",
                    getFid(),
                    last1min,
                    msgProcessedMeter.getOneMinuteRate());*/

                Boolean reqLB = false;
                //double a80 = (snp.get95thPercentile() * durationFactor +
                //snp.get75thPercentile() * durationFactor) / 2.0;
                double a80 = snp.get95thPercentile();

            /*if (stableenough(starttime)) {
                if (a80 > qLenThreshold) {
                    LOGGER.error("Initiating loadbalancing.");
                    reqLB = true;
                }
                starttime = System.currentTimeMillis();
            }*/

                LOGGER.info("Checkpointing State");
                //send incremental checkpoint to the neighbor.
                synchronized (this) {
                    for (FlakeToken neighbor
                            : peerMonitor.getNeighborsToBackupOn().values()) {
                        byte[] chkpointdata
                                = stateManager.getIncrementalStateCheckpoint(
                                neighbor.getFlakeID());
                        stateSocSender.sendMore(getFid());
                        stateSocSender.sendMore(done.toString());
                        stateSocSender.sendMore(reqLB.toString());
                        stateSocSender.send(chkpointdata, 0);
                    }
                }


            }
        }

        stateSocSender.close();
        monitor.interrupt();
        notifyStopped(true);
    }

    /**
     * Checks if enough time has passed (say a min).
     * @param starttime start time since when the time must be checked.
     * @return true if the time passed is more than a min.
     */
    private boolean stableenough(final long starttime) {
        long now = System.currentTimeMillis();
        final int secs = 30;
        final int th = 1000;
        if ((now - starttime) / th >= secs) {
            return true;
        }
        return false;
    }

    /**
     * The function is called whenever a peer is added or removed for this
     * flake.
     *
     * @param newPeers     list of newly added peers.
     * @param removedPeers list of removed peers.
     */
    @Override
    public final void peerListUpdated(
            final SortedMap<Integer, FlakeToken> newPeers,
            final SortedMap<Integer, FlakeToken> removedPeers) {
        updateStateSubscriptions(newPeers, removedPeers);
    }


    /**
     * Update state subscriptions.
     * @param neighborsToAdd newNeighbors to subscribe and connect to.
     * @param neighborsToRemove old neighbors to remove from the subscription.
     */
    private void updateStateSubscriptions(
            final SortedMap<Integer, FlakeToken> neighborsToAdd,
            final SortedMap<Integer, FlakeToken> neighborsToRemove) {
        /**
         * ZMQ socket connection publish the state to the backups.
         */
        if (neighborsToAdd != null) {
            for (Map.Entry<Integer, FlakeToken> finfo
                    : neighborsToAdd.entrySet()) {

                String ssConnetStr
                        = Utils.Constants.FLAKE_STATE_SUB_SOCK_PREFIX
                        + finfo.getValue().getIpOrHost() + ":"
                        + finfo.getValue().getStateCheckptPort();

                //stateSocReceiver.subscribe(finfo.getValue().getBytes());


                //LOGGER.error("{} SUBING FOR {}", getFid(), finfo.getValue());
                LOGGER.info("connecting STATE CHECKPOINTER "
                        + "to listen for state updates: {}", ssConnetStr);

                stateSocReceiver.connect(ssConnetStr);
            }
        }

        if (neighborsToRemove != null) {
            for (Map.Entry<Integer, FlakeToken> finfo
                    : neighborsToRemove.entrySet()) {

                String ssConnetStr
                        = Utils.Constants.FLAKE_STATE_SUB_SOCK_PREFIX
                        + finfo.getValue().getIpOrHost() + ":"
                        + finfo.getValue().getStateCheckptPort();

                //stateSocReceiver.unsubscribe(finfo.getValue().getBytes());

                LOGGER.info("DISCONNECTING STATE CHECKPOINTER "
                        + "to listen for state updates: {}", ssConnetStr);

                stateSocReceiver.disconnect(ssConnetStr);
            }
        }
    }
}
