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
import edu.usc.pgroup.floe.container.FlakeControlCommand;
import edu.usc.pgroup.floe.flake.messaging.FlakeMessageReceiver;
import edu.usc.pgroup.floe.flake.messaging.FlakeMessageSender;
import edu.usc.pgroup.floe.utils.SystemSignal;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;

/**
 * @author kumbhare
 */
public class Flake {
    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(Flake.class);

    /**
     * The id of the flake.
     */
    private final String flakeId;

    /**
     * Application name to which this flake belongs.
     */
    private final String appName;

    /**
     * Application's jar file name stored in the container's local store.
     */
    private final String appJar;

    /**
     *
     */
    //private final int[] ports;

    /**
     * the map of pellet to ports to start the zmq sockets.
     * one for each edge in the application graph.
     */
    private final Map<String, Integer> pelletPortMap;

    /**
     * the map of pellet to ports to start the zmq sockets for the backchannel.
     * one for each edge in the application graph.
     */
    private final Map<String, Integer> pelletBackChannelPortMap;

    /**
     * the map of pellet to list of streams that pellet is subscribed to.
     */
    private final Map<String, List<String>> pelletStreamsMap;

    /**
     * Recurring timer for sending heartbeats.
     */
    private Timer heartBeatTimer;

    /**
     * Flake heartbeat task.
     */
    private FlakeHeartbeatTask flakeHeartbeatTask;

    /**
     * the flakeInfo object sent during heartbeats.
     */
    private FlakeInfo flakeInfo;

    /**
     * The flake receiver object, responsible for listening for and receiving
     * messages from the preceding pellets in the graph.
     */
    private FlakeMessageReceiver flakeRecevier;

    /**
     * The flake sender object responsible for sending message to the
     * succeeding pellets in the graph.
     */
    private FlakeMessageSender flakeSender;

    /**
     * Shared ZMQ Context.
     */
    private final ZMQ.Context sharedContext;

    /**
     * Container id.
     */
    private final String containerId;

    /**
     * Pellet id/name.
     */
    private final String pelletId;

    /**
     * the kill socket used while terminating the flake.
     */
    private ZMQ.Socket killsock;

    /**
     * List of pellet instances running on the flake.
     */
    private final List<PelletExecutor> runningPelletInstances;

    /**
     * Constructor.
     * @param pid pellet id/name.
     * @param fid flake's id. (the container decides a unique id for the
     *                flake)
     * @param cid container's id. This will be appended by fid to get the
     *            actual globally unique flake id. This is to support
     *            psuedo-distributed mode with multiple containers. Bug#1.
     * @param app application's name to which this flake belongs.
     * @param jar the application's jar file name.
     * @param portMap the map of ports on which this flake should
     *                       listen on. Note: This is fine here (and not as a
     *                       control signal) because this depends only on
     *                       static application configuration and not on
     * @param backChannelPortMap map of port for the backchannel. One port
     *                           per target pellet.
     * @param streamsMap map from successor pellets to subscribed
     *                         streams.
     */
    public Flake(final String pid,
                 final String fid,
                 final String cid,
                 final String app,
                 final String jar,
                 final Map<String, Integer> portMap,
                 final Map<String, Integer> backChannelPortMap,
                 final Map<String, List<String>> streamsMap) {
        this.flakeId = Utils.generateFlakeId(cid, fid);
        this.containerId = cid;
        this.pelletPortMap = portMap;
        this.pelletBackChannelPortMap = backChannelPortMap;
        this.pelletStreamsMap = streamsMap;
        this.appName = app;
        this.appJar = jar;
        this.sharedContext = ZMQ.context(Utils.Constants.FLAKE_NUM_IO_THREADS);
        this.pelletId = pid;
        this.runningPelletInstances = new ArrayList<>();
    }

    /**
     * @return the flake's id.
     */
    public final String getFlakeId() {
        return flakeId;
    }


    /**
     * @return pellet id (same as name).
     */
    public final String getPelletId() {
        return pelletId;
    }

    /**
     * Starts the server and the schedules the heartbeats.
     */
    public final void start() {
        LOGGER.info("Initializing flake.");
        initializeFlake();
    }

    /**
     * Start receiving data from the predecessor.
     */
    private void startFlakeReciever() {
        flakeRecevier = new FlakeMessageReceiver(sharedContext, this);
        flakeRecevier.start();
    }

    /**
     * Start receiving data from the predecessor.
     */
    private void startFlakeSender() {
        flakeSender = new FlakeMessageSender(sharedContext, flakeId,
                pelletPortMap, pelletBackChannelPortMap, pelletStreamsMap);
        flakeSender.start();
    }

    /**
     * Initializes the flake. Including:
     * setup the flakeInfo object (for heartbeat)
     */
    private void initializeFlake() {
        flakeInfo = new FlakeInfo(pelletId, flakeId, containerId, appName);
        flakeInfo.setStartTime(new Date().getTime());

        LOGGER.info("Setting up Flake Receiver");
        startFlakeReciever();

        LOGGER.info("Start the command receiver");
        startFlakeSender();

        LOGGER.info("Scheduling flake heartbeat.");
        flakeHeartbeatTask = new FlakeHeartbeatTask(flakeInfo, sharedContext);
        scheduleHeartBeat();

        LOGGER.info("Initializing kill socket for Flake.");
        killsock  = sharedContext.socket(ZMQ.PUB);
        killsock.bind(
                Utils.Constants.FLAKE_KILL_CONTROL_SOCK_PREFIX
                        + flakeId);

        LOGGER.info("flake Started: {}.", getFlakeId());
    }

    /**
     * Terminates all the relevant threads of the flake.
     */
    private void terminateFlake() {
        if (runningPelletInstances.size() > 0) {
            LOGGER.error("Cannot terminate. Pellets are running. Use "
                    + "Decrement Pellet command to kill all pellet "
                    + "instances.");
            return;
        }

        LOGGER.info("Sending Kill Signal to receiver/sender Flake.");
        byte[] dummy = new byte[]{1};
        killsock.sendMore(Utils.Constants.PUB_ALL);
        killsock.send(dummy, 0);

        //stop heartbeat.
        flakeHeartbeatTask.setCancelled();

        //close kill sock.
        killsock.close();
    }

    /**
     * Schedules and starts recurring the flake heartbeat.
     */
    private void scheduleHeartBeat() {
        long delay = FloeConfig.getConfig().getInt(ConfigProperties
                .FLAKE_HEARTBEAT_PERIOD) * Utils.Constants.MILLI;
        if (heartBeatTimer == null) {
            heartBeatTimer = new Timer();
            heartBeatTimer.scheduleAtFixedRate(flakeHeartbeatTask
                    , 0
                    , delay);
            LOGGER.info("Heartbeat scheduled with period: "
                    + FloeConfig.getConfig().getInt(
                    ConfigProperties.FLAKE_HEARTBEAT_PERIOD)
                    + " seconds");
        }
    }

    /**
     * Create a new pellet instance.
     * @param p the deserialized pellet instance received from the user.
     */
    public final void incrementPellet(final byte[] p) {
        LOGGER.info("Starting pellet");
        int nextPEIdx = 0;

        if (runningPelletInstances.size() > 0) {
            nextPEIdx = runningPelletInstances.get(
                    runningPelletInstances.size() - 1)
                    .getPelletInstanceIndex() + 1;
        }

        PelletExecutor pe = new PelletExecutor(nextPEIdx, p, appName, appJar,
                flakeId,
                sharedContext);

        runningPelletInstances.add(pe);
        pe.start();
    }

    /**
     * @return the application name.
     */
    public final String getAppName() {
        return appName;
    }

    /**
     * @return the application's jar name.
     */
    public final String getAppJar() {
        return appJar;
    }

    /**
     * Process control signal received from the container.
     * @param command Flake Command.
     * @param signal Signal socket to be used to send signals to the pellet
     *               instances. //ugly.. :( find a better way.
     * @return the result after processing the command.
     */
    public final byte[] processControlSignal(
            final FlakeControlCommand command,
            final ZMQ.Socket signal) {

        LOGGER.warn("Processing command: " + command);
        switch (command.getCommand()) {
            case INCREMENT_PELLET:
                byte[] bpellet = (byte[]) command.getData();
                LOGGER.info("CREATING PELLET: on " + getFlakeId());
                incrementPellet(bpellet);
                break;
            case DECREMENT_PELLET:
                String dpid = (String) command.getData();
                LOGGER.info("REMOVING PELLET: " + dpid + " on "
                        + getFlakeId());
                if (runningPelletInstances.size() > 0) {
                    //NEED TO DO ERROR HANDLING HERE.
                    PelletExecutor insToRemove
                            = runningPelletInstances.remove(0);
                    signal.sendMore(insToRemove.getPelletInstanceId());
                    SystemSignal systemSignal = new SystemSignal(appName,
                            pelletId,
                            SystemSignal.SystemSignalType.KillInstance,
                            null);
                    signal.send(Utils.serialize(systemSignal), 0);
                } else {
                    LOGGER.error("Flake {} does not have any running pellet "
                            + "instances.", getFlakeId());
                }
                //decrementPellet();
                break;
            case TERMINATE:
                //No pellet should be running
                terminateFlake();
                break;
            default:
                LOGGER.warn("Unrecognized command: " + command);
                break;
        }

        //Get valid results here. Must define a results format.
        byte[] result = new byte[]{'1'};
        return result;
    }
}
