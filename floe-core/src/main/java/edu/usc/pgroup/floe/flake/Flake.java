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

import com.codahale.metrics.CsvReporter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.app.Pellet;
import edu.usc.pgroup.floe.container.FlakeControlCommand;
import edu.usc.pgroup.floe.flake.coordination.CoordinationComponent;
import edu.usc.pgroup.floe.flake.coordination.CoordinationManagerFactory;
import edu.usc.pgroup.floe.flake.coordination.ReducerCoordinationComponent;
import edu.usc.pgroup.floe.flake.messaging.MsgReceiverComponent;
import edu.usc.pgroup.floe.flake.messaging.sender.SenderFEComponent;
import edu.usc.pgroup.floe.flake.statemanager.StateManagerComponent;
import edu.usc.pgroup.floe.flake.statemanager.StateManagerFactory;
import edu.usc.pgroup.floe.resourcemanager.ResourceMapping;
import edu.usc.pgroup.floe.signals.SystemSignal;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.thriftgen.TPellet;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.hyperic.sigar.Sigar;
import org.hyperic.sigar.SigarException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Timer;
import java.util.concurrent.TimeUnit;

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
     * the map of pellet to ports to start the zmq sockets for the dispersion.
     * one for each edge in the application graph.
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
     * Map of src pellet to channel type (one per edge).
     */
    private final Map<String, String> predPelletChannelTypeMap;

    /**
     * Port to be used for sending checkpoint data.
     */
    private final int stateChkptPort;


    /**
     * Recurring timer for sending heartbeats.
     */
    private Timer heartBeatTimer;

    /**
     * Flake heartbeat task.
     */
    private FlakeHeartbeatComponent flakeHeartbeatComponent;

    /**
     * the flakeInfo object sent during heartbeats.
     */
    private FlakeInfo flakeInfo;

    /**
     * The flake receiver object, responsible for listening for and receiving
     * messages from the preceding pellets in the graph.
     */
    //private FlakeMessageReceiver2 flakeRecevier;

    /**
     * The flake sender object responsible for sending message to the
     * succeeding pellets in the graph.
     */
    //private FlakeMessageSender2 flakeSender;

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
     * Shutdown hook to close sockets etc on Cntl-C or unexpected shutdown.
     */
    private Thread shutdownHook;

    /**
     * List of pellet instances running on the flake.
     */
    private final List<PelletExecutor> runningPelletInstances;

    /**
     * The state manager component.
     */
    private StateManagerComponent stateManager;

    /**
     * The local coordination manager.
     */
    private CoordinationComponent coordinationManager;

    /**
     * Flake's token on the ring.
     */
    private int myToken;

    /**
     * The user pellet object including all alternates.
     */
    private TPellet tPellet;

    /**
     * The flake's message sender component's frontend.
     */
    private SenderFEComponent flakeSenderComponent;

    /**
     * Flake's message receiver component.
     */
    private MsgReceiverComponent flakeReceiverComponent;

    /**
     * Metric registry for this flake.
     */
    private MetricRegistry metricRegistry;

    /**
     * Metrics Reporter.
     */
    private CsvReporter reporter;

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
     * @param statePort       Port to be used for sending checkpoint data.
     * @param portMap the map of ports on which this flake should
     *                       listen on. Note: This is fine here (and not as a
     *                       control signal) because this depends only on
     *                       static application configuration and not on
     * @param backChannelPortMap map of port for the dispersion. One port
     *                           per target pellet.
     * @param successorChannelTypeMap Map of target pellet to channel type
     *                                (one per edge)
     * @param predChannelTypeMap Map of src pellet to channel type
     *                                (one per edge)
     * @param streamsMap map from successor pellets to subscribed
     *                         streams.
     */
    public Flake(final String pid,
                 final String fid,
                 final String cid,
                 final String app,
                 final String jar,
                 final int statePort,
                 final Map<String, Integer> portMap,
                 final Map<String, Integer> backChannelPortMap,
                 final Map<String, String> successorChannelTypeMap,
                 final Map<String, String> predChannelTypeMap,
                 final Map<String, List<String>> streamsMap) {
        this.flakeId = Utils.generateFlakeId(cid, fid);
        this.containerId = cid;
        this.pelletPortMap = portMap;
        this.pelletBackChannelPortMap = backChannelPortMap;
        this.pelletChannelTypeMap = successorChannelTypeMap;
        this.predPelletChannelTypeMap = predChannelTypeMap;
        this.pelletStreamsMap = streamsMap;
        this.appName = app;
        this.appJar = jar;
        this.sharedContext = ZMQ.context(Utils.Constants.FLAKE_NUM_IO_THREADS);
        this.pelletId = pid;
        this.runningPelletInstances = new ArrayList<>();
        this.stateChkptPort = statePort;

        this.metricRegistry = new MetricRegistry();

        final int reporterPeriod = 1;


        File metricDir = new File("./metrics/" + flakeId);
        if (!metricDir.exists()) {
            metricDir.mkdirs();
        }


        this.reporter = CsvReporter.forRegistry(metricRegistry)
                .convertRatesTo(TimeUnit.SECONDS)
                .convertDurationsTo(TimeUnit.MILLISECONDS)
                .build(metricDir);
        reporter.start(reporterPeriod, TimeUnit.SECONDS);

        metricRegistry.register(
                MetricRegistry.name(Flake.class, "CPU"),
                new Gauge<Double>() {

                    /**
                     * Sigar object used to retrieve cpu usage.
                     */
                    private final Sigar sigar = new Sigar();

                    /**
                     * @return the instantaneous CPU usage.
                     */
                    @Override
                    public Double getValue() {
                        double cpuUsage = 0;

                        try {
                            cpuUsage = sigar.getCpuPerc().getCombined();
                        } catch (SigarException e) {
                            LOGGER.warn("Could not retrieve cpu usage. {}", e);
                        }

                        return cpuUsage;
                    }
                }
        );
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
     * This is called by the flake service on creating a new flake object.
     * (before initialize)
     */
    public final void start() {
        LOGGER.info("starting flake.");
        flakeInfo = new FlakeInfo(pelletId, flakeId, containerId, appName);
        flakeInfo.setStartTime(new Date().getTime());

        this.myToken = new Random(System.nanoTime()).nextInt();
        ZKUtils.updateToken(appName,
                pelletId,
                flakeId,
                myToken,
                stateChkptPort);

        //start heartbeat
        LOGGER.info("Scheduling flake heartbeat.");
        flakeHeartbeatComponent = new FlakeHeartbeatComponent(
                metricRegistry, flakeInfo,
                flakeId, "HEAET-BEAT", sharedContext);
        flakeHeartbeatComponent.startAndWait();

        LOGGER.info("Flake started. Starting control channel.");
        startControlChannel();


        if (flakeHeartbeatComponent != null) {
            flakeHeartbeatComponent.stopAndWait();
        }

        if (flakeReceiverComponent != null) {
            flakeReceiverComponent.stopAndWait();
        }

        if (stateManager != null) {
            stateManager.stopAndWait();
        }

        if (coordinationManager != null) {
            coordinationManager.stopAndWait();
        }

        if (flakeSenderComponent != null) {
            flakeSenderComponent.stopAndWait();
        }

        //initializeFlake();
    }

    /**
     * Initializes the flake. SHOULD BE A SYNCHRONOUS FUNCTION. i.e. when the
     * function returns, the flake should be fully initialized.
     * This is called after receiving an initialize signal from the container.
     * (After start)
     */
    private void initializeFlake() {

        //get the application configuration.
        ResourceMapping resourceMapping
                = ZKUtils.getResourceMapping(appName);

        TFloeApp tfloeApp = resourceMapping.getFloeApp();

        tPellet = tfloeApp.get_pellets().get(pelletId);

        //FixeME: Change the way alternates are handled later. For now we
        // just choose the active alternate.

        byte[] activeAlternate = tPellet.get_alternates().get(
                tPellet.get_activeAlternate()
        ).get_serializedPellet();

        //deserialize the active alternate and get the runnable pellet object
        // out of it.
        Pellet pellet = deserializePellet(activeAlternate);



        //Start the state manager.
        LOGGER.info("Starting state manager.");
        stateManager = StateManagerFactory.getStateManager(metricRegistry,
                pellet,
                flakeId, "STATE-MANAGER", sharedContext, stateChkptPort);

        if (stateManager != null) {
            stateManager.startAndWait();
        }

        //Start the coordination manager.
        LOGGER.info("Starting coordination manager.");
        coordinationManager = CoordinationManagerFactory
                .getCoordinationManager(metricRegistry, appName,
                        pelletId,
                        pellet,
                        flakeId, myToken,
                        "COORDINATION-MANAGER", stateManager, sharedContext);

        if (coordinationManager != null) {
            coordinationManager.startAndWait();
        }


        LOGGER.info("Start the command receiver.");
        flakeSenderComponent = new SenderFEComponent(
                metricRegistry,
                sharedContext,
                appName,
                pelletId,
                flakeId,
                "FLAKE-SENDER",
                pelletPortMap,
                pelletBackChannelPortMap,
                pelletChannelTypeMap,
                pelletStreamsMap
        );
        flakeSenderComponent.startAndWait();

        LOGGER.info("Setting up Flake Receiver");
        flakeReceiverComponent
                = new MsgReceiverComponent(
                metricRegistry, flakeId, "MSG-RECEIVER",
                sharedContext, predPelletChannelTypeMap, myToken);
        flakeReceiverComponent.startAndWait();

        LOGGER.info("Finished flake execution. {}", flakeId);
    }

    /**
     * Create a new pellet instance.
     * @param p the deserialized pellet instance received from the user.
     * @return the pellet instance id of the newly created pellet.
     */
    public final String incrementPellet(final byte[] p) {
        LOGGER.info("Starting pellet");
        int nextPEIdx = 0;

        if (runningPelletInstances.size() > 0) {
            nextPEIdx = runningPelletInstances.get(
                    runningPelletInstances.size() - 1)
                    .getPelletInstanceIndex() + 1;
        }

        Pellet pellet = deserializePellet(p);

        PelletExecutor pe = new PelletExecutor(metricRegistry, nextPEIdx,
                pellet, flakeId, sharedContext, stateManager);

        /*PelletExecutor pe = new PelletExecutor(nextPEIdx, p, appName, appJar,
                flakeId,
                sharedContext, stateManager);*/

        runningPelletInstances.add(pe);
        pe.start();
        flakeInfo.incrementPellets();
        return pe.getPelletInstanceId();
    }

    /**
     * Deserializes the pellet using the given appjar.
     * @param p the serialized pellet.
     * @return THe deserialized pellet.
     */
    private Pellet deserializePellet(final byte[] p) {
        URLClassLoader loader;
        Pellet pellet = null;
        try {
            File relativeJarLoc = new File(
                    Utils.getContainerJarDownloadPath(appName, appJar));

            URL jarLoc = new URL(
                    "file://" + relativeJarLoc.getAbsolutePath());

            LOGGER.info("Loading jar: {} into class loader.", jarLoc);
            loader = URLClassLoader.newInstance(
                    new URL[]{jarLoc},
                    getClass().getClassLoader()
            );

            pellet = (Pellet) Utils.deserialize(p, loader);
        } catch (MalformedURLException e) {
            e.printStackTrace();
            LOGGER.error("Invalid Jar URL Exception: {}", e);
        }

        return pellet;
    }

    /**
     * @return the application name.
     */
    public final String getAppName() {
        return appName;
    }

    /**
     * Starts the control channel.
     */
    private void startControlChannel() {

        LOGGER.info("Starting inproc socket to send signals to pellets: "
                + Utils.Constants.FLAKE_RECEIVER_SIGNAL_BACKEND_SOCK_PREFIX
                + flakeId);
        final ZMQ.Socket signal = sharedContext.socket(ZMQ.PUB);
        signal.bind(
                Utils.Constants.FLAKE_RECEIVER_SIGNAL_BACKEND_SOCK_PREFIX
                        + flakeId);

        LOGGER.info("Starting backend ipc socket for control channel at: "
                + Utils.Constants.FLAKE_CONTROL_SOCK_PREFIX
                + flakeId);
        final ZMQ.Socket controlSocket = sharedContext.socket(ZMQ.REP);
        controlSocket.connect(
                Utils.Constants.FLAKE_CONTROL_SOCK_PREFIX
                        + flakeId);

        LOGGER.info("Starting backend ipc socket for control channel at: "
                + Utils.Constants.FLAKE_RECEIVER_CONTROL_FWD_PREFIX
                + flakeId);
        final ZMQ.Socket msgReceivercontrolForwardSocket
                = sharedContext.socket(ZMQ.REQ);
        msgReceivercontrolForwardSocket.connect(
                Utils.Constants.FLAKE_RECEIVER_CONTROL_FWD_PREFIX
                        + flakeId);

        boolean done = false;
        while (!done && !Thread.currentThread().isInterrupted()) {
            byte[] message = controlSocket.recv();
            byte[] result = new byte[]{1};

            //process control message.
            FlakeControlCommand command
                    = (FlakeControlCommand) Utils.deserialize(
                    message);

            LOGGER.info("Received command: " + command);
            switch (command.getCommand()) {
                case INITIALIZE:
                    initializeFlake();
                    break;
                case CONNECT_PRED:
                case DISCONNECT_PRED:
                    //Send to the receiver.
                    LOGGER.info("CONNECT/DISCONNECT COMMAND RECEIVED.");
                    msgReceivercontrolForwardSocket.send(message, 0);
                    result = msgReceivercontrolForwardSocket.recv();
                    break;
                case PELLET_SIGNAL:
                    //forward singal to the pellet.
                    LOGGER.info("Received signal for: "
                            + flakeId);
                    signal.sendMore(Utils.Constants.PUB_ALL);
                    signal.send((byte[]) command.getData(), 0);
                    break;
                case SWITCH_ALTERNATE:
                    //create a switch alternate signal and send to pellets.
                    LOGGER.info("Switching alternate for: "
                            + flakeId);
                    SystemSignal systemSignal = new SystemSignal(
                            getAppName(),
                            getPelletId(),
                            SystemSignal.SystemSignalType.SwitchAlternate,
                            (byte[]) command.getData()
                    );
                    signal.sendMore(Utils.Constants.PUB_ALL);
                    signal.send(Utils.serialize(systemSignal), 0);
                    break;
                case TERMINATE:
                    if (runningPelletInstances.size() == 0) {
                        done = true; //aaahh.. bug.. this will send the reply
                        // before stopping the components.
                    } else {
                        LOGGER.warn("Flake has running pellets. "
                                + "Cannot terminate");
                    }
                    break; //?? Do we need anything else? Prob. Not.
                default:
                    processControlSignal(command,
                            signal, msgReceivercontrolForwardSocket);
            }

            controlSocket.send(result, 0);
        }
    }

    /**
     * Process control signal received from the container.
     * @param command Flake Command.
     * @param signal Signal socket to be used to send signals to the pellet
     *               instances. //ugly.. :( find a better way.
     * @param msgReceivercontrolForwardSocket socket to forward command (or
     *                                        part of it to the flake receiver).
     * @return the result after processing the command.
     */
    public final byte[] processControlSignal(
            final FlakeControlCommand command,
            final ZMQ.Socket signal,
            final ZMQ.Socket msgReceivercontrolForwardSocket) {

        LOGGER.warn("Processing command: " + command);
        byte[] result = new byte[]{1};
        FlakeControlCommand newCommand;

        switch (command.getCommand()) {
            case INCREMENT_PELLET:
                byte[] bpellet = (byte[]) command.getData();
                LOGGER.info("CREATING PELLET: on " + getFlakeId());
                String peId = incrementPellet(bpellet);
                newCommand = new FlakeControlCommand(
                        FlakeControlCommand.Command.INCREMENT_PELLET,
                        peId
                );
                msgReceivercontrolForwardSocket.send(
                        Utils.serialize(newCommand), 0);
                msgReceivercontrolForwardSocket.recv();

                //NOTE: WE SHOULD DO THIS ON SPECIAL COMMAND. BUT
                // DOING IT HERE JUST TO TEST.
                if (coordinationManager
                        instanceof ReducerCoordinationComponent) {

                    List<String> neighbors = ((ReducerCoordinationComponent)
                            coordinationManager).getNeighborsToBackupMsgsFor();
                    newCommand = new FlakeControlCommand(
                            FlakeControlCommand.Command.UPDATE_SUBSCRIPTION,
                            neighbors
                            );
                    msgReceivercontrolForwardSocket.send(
                            Utils.serialize(newCommand), 0);
                    msgReceivercontrolForwardSocket.recv();
                }
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
                    //notifyPelletRemoved(insToRemove.getPelletInstanceId());

                    flakeInfo.decrementPellets();

                    newCommand = new FlakeControlCommand(
                            FlakeControlCommand.Command.DECREMENT_PELLET,
                            insToRemove.getPelletInstanceId()
                    );
                    msgReceivercontrolForwardSocket.send(
                            Utils.serialize(newCommand), 0);
                    result = msgReceivercontrolForwardSocket.recv();
                } else {
                    LOGGER.error("Flake {} does not have any running pellet "
                            + "instances.", getFlakeId());
                }
                //decrementPellet();

                break;
            case DECREMENT_ALL_PELLETS:

                LOGGER.info("REMOVING ALL PELLETS: on " + getFlakeId());
                while (runningPelletInstances.size() > 0) {
                    //NEED TO DO ERROR HANDLING HERE.
                    PelletExecutor insToRemove
                            = runningPelletInstances.remove(0);
                    signal.sendMore(insToRemove.getPelletInstanceId());
                    SystemSignal systemSignal = new SystemSignal(appName,
                            pelletId,
                            SystemSignal.SystemSignalType.KillInstance,
                            null);
                    signal.send(Utils.serialize(systemSignal), 0);
                    //notifyPelletRemoved(insToRemove.getPelletInstanceId());
                    newCommand = new FlakeControlCommand(
                            FlakeControlCommand.Command.DECREMENT_PELLET,
                            insToRemove.getPelletInstanceId()
                    );
                    msgReceivercontrolForwardSocket.send(
                            Utils.serialize(newCommand), 0);
                    result = msgReceivercontrolForwardSocket.recv();
                }

                LOGGER.error("Flake {} does not have any running pellet "
                            + "instances.", getFlakeId());
                break;
            case START_PELLETS:
                LOGGER.info("STARTING PELLETS: on " + getFlakeId());
                if (runningPelletInstances.size() > 0) {
                    for (PelletExecutor peInstance: runningPelletInstances) {
                        signal.sendMore(peInstance.getPelletInstanceId());
                        SystemSignal systemSignal = new SystemSignal(appName,
                                pelletId,
                                SystemSignal.SystemSignalType.StartInstance,
                                null);
                        signal.send(Utils.serialize(systemSignal), 0);
                    }
                } else {
                    LOGGER.error("Flake {} does not have any running pellet "
                            + "instances.", getFlakeId());
                }
                break;
            default:
                LOGGER.warn("Unrecognized command: " + command);
                break;
        }

        //Get valid results here. Must define a results format.
        return result;
    }
}
