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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.app.AppContext;
import edu.usc.pgroup.floe.app.EmitterEnvelopeHook;
import edu.usc.pgroup.floe.app.pellets.IteratorPellet;
import edu.usc.pgroup.floe.app.pellets.PelletContext;
import edu.usc.pgroup.floe.app.pellets.Signallable;
import edu.usc.pgroup.floe.app.pellets.TupleItertaor;
import edu.usc.pgroup.floe.flake.messaging.MessageEmitter;
import edu.usc.pgroup.floe.flake.messaging.MsgReceiverComponent;
import edu.usc.pgroup.floe.flake.statemanager.StateManager;
import edu.usc.pgroup.floe.serialization.SerializerFactory;
import edu.usc.pgroup.floe.serialization.TupleSerializer;
import edu.usc.pgroup.floe.signals.PelletSignal;
import edu.usc.pgroup.floe.signals.SystemSignal;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.net.URLClassLoader;
import java.nio.charset.Charset;

/**
 * The pellet executor class.
 * @author kumbhare
 */
public class PelletExecutor extends Thread {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PelletExecutor.class);

    /**
     * ZMQ Context.
     */
    private final ZMQ.Context context;

    /**
     * Corresponding flake's id.
     */
    private final String flakeId;

    /**
     * pelletInstanceId (has to be unique). Best
     * practice: use flakeid-intancecount on the
     * flake as the pellet id.
     */
    private final String pelletInstanceId;

    /**
     * Pellet instance index on the given flake.
     */
    private final int pelletInstanceIndex;

    /**
     *  The common state manager object. This is one per
     *                     flake and common fro all pellet instances. Should
     *                     be thread safe.
     */
    private final StateManager pelletStateManager;

    /**
     * Tuple iterator used by the pellets.
     */
    private final TupleItertaor tupleIterator;

    /**
     * Metric registyr.
     */
    private final MetricRegistry metricRegistry;

    /**
     * Data receiver object.
     */
    private final ZMQ.Socket dataReceiver;

    /**
     * Shared app context.
     */
    private final AppContext appContext;

    /**
     * The fully qualified pellet class name.
     */
    private String pelletClass;


    /**
     * Instance of the pellet class.
     */
    private IteratorPellet pellet;


    /**
     * Pellet id/name as it apprears in the toplogy.
     */
    private String pelletName;

    /**
     * The emmitter object associated with this pellet.
     */
    private MessageEmitter emitter;

    /**
     * Serializer to be used to serialize and deserialize the data tuples.
     */
    private final TupleSerializer tupleSerializer;


    /**
     * Indicates whether a "Kill" system signal has been received.
     */
    private boolean killSignalReceived;

    /**
     * Class loader for loading pellet classes from a jar file.
     */
    private URLClassLoader loader;

    /**
     * Pellet context.
     */
    private final PelletContext pelletContext;

    /**
     * Flag indicating if the pellet has been started.
     */
    private Boolean started;


    /**
     * hiding default constructor.
     * @param registry Metrics registry used to log various metrics.
     * @param pelletIndex flake-unique pellet index (need not be contiguous)
     * flake as the pellet id.
     * @param sharedContext shared ZMQ context to be used in inproc comm. for
 *                      receiving message from the flake.
     * @param fid flake's id to which this pellet belongs.
     * @param pid pellet's user defined name
     * @param stateManager The common state manager object. This is one per
*                     flake and common fro all pellet instances. Should
*                     be thread safe.
     * @param appName Application's name.
     * @param appCtx application context.
     */
    private PelletExecutor(
            final MetricRegistry registry,
            final int pelletIndex,
            final ZMQ.Context sharedContext,
            final String fid,
            final String pid,
            final StateManager stateManager,
            final String appName,
            final AppContext appCtx) {
        this.context = sharedContext;
        this.tupleSerializer = SerializerFactory.getSerializer();
        this.flakeId = fid;
        this.pelletInstanceId = fid + "-" + pelletIndex;
        this.pelletInstanceIndex = pelletIndex;
        this.killSignalReceived = false;
        this.pelletStateManager = stateManager;
        this.metricRegistry = registry;
        this.pelletName = pid;
        this.pelletContext = new PelletContext(
                appName, pelletInstanceId,
                pelletName, flakeId, metricRegistry);
        this.dataReceiver = context.socket(ZMQ.SUB);
        //CONNECT HERE.. :) but wait for Start command before reading data..
        //Let it be buffered till then.
        this.dataReceiver.connect(
                Utils.Constants.FLAKE_RECEIVER_BACKEND_SOCK_PREFIX
                        + flakeId);
        this.dataReceiver.subscribe(pelletInstanceId.getBytes());

        this.started = false;
        this.tupleIterator = new TupleItertaor(pelletInstanceId,
                pelletStateManager, tupleSerializer, dataReceiver);
        this.appContext = appCtx;
    }

    /**
     * Construct pellet instance from fqdn.
     * @param registry Metrics registry used to log various metrics.
     * @param pelletIndex flake-unique pellet index (need not be contiguous)
     * flake as the pellet id.
     * @param fqdnClass the fully qualified class name for the pellet.
     * @param sharedContext shared ZMQ context to be used in inproc comm. for
     *                      receiving message from the flake.
     * @param fid flake's id to which this pellet belongs.
     * @param stateManager The common state manager object. This is one per
     *                     flake and common fro all pellet instances. Should
     *                     be thread safe.
     * @param pid pellet's user defined name
     * @param appName Application's context.
     *
    public PelletExecutor(final MetricRegistry registry,
                          final int pelletIndex,
                    final String fqdnClass, final String fid, final String
                                  pid, final String appName,
                    final ZMQ.Context sharedContext,
                    final StateManager stateManager) {
        this(registry, pelletIndex, sharedContext,
                fid, pid, stateManager, appName);
        this.pelletClass = fqdnClass;
        this.pellet = (IteratorPellet) Utils.instantiateObject(pelletClass);
        //this.pellet.setup(null, new PelletContext(pelletInstanceId));
    }*/



    /**
     * Construct pellet instance from de-serialized version.
     * @param registry Metrics registry used to log various metrics.
     * @param pelletIndex flake-unique pellet index (need not be contiguous)
     * flake as the pellet id.
     * @param p pellet instance from the user.
     * @param fid flake's id to which this pellet belongs.
     * @param pid pellet's user defined name.
     * @param appName Application's context.
     * @param sharedContext shared ZMQ context to be used in inproc comm. for
*                      receiving message from the flake.
     * @param stateManager The common state manager object. This is one per
*                     flake and common fro all pellet instances. Should
*                     be thread safe.
     * @param appCtx shared app context among all pellet instances.
     */
    public PelletExecutor(final MetricRegistry registry,
                          final int pelletIndex,
                          final IteratorPellet p,
                          final String fid, final String pid,
                          final String appName,
                          final ZMQ.Context sharedContext,
                          final StateManager stateManager,
                          final AppContext appCtx) {
        this(registry, pelletIndex, sharedContext,
                fid, pid, stateManager, appName, appCtx);
        this.pellet = p;
        //this.pellet.setup(null, new PelletContext(pelletInstanceId));
    }

    /**
     * @param pelletIndex flake-unique pellet index (need not be contiguous)
     * flake as the pellet id.
     * @param p serialized pellet
     * @param app application name
     * @param appJar application's jar file namne.
     * @param fid flake id
     * @param sharedContext shared zmq contex.
     * @param stateManager The common state manager object. This is one per
     *                     flake and common fro all pellet instances. Should
     *                     be thread safe.
     *
    public PelletExecutor(final int pelletIndex,
                          final byte[] p,
                          final String app,
                          final String appJar,
                          final String fid,
                          final ZMQ.Context sharedContext,
                          final StateManagerComponent stateManager) {

        this(pelletIndex, sharedContext, fid, stateManager);
        try {
            File relativeJarLoc = new File(
                    Utils.getContainerJarDownloadPath(app, appJar));

            URL jarLoc = new URL(
                    "file://" + relativeJarLoc.getAbsolutePath());

            LOGGER.info("Loading jar: {} into class loader.", jarLoc);
            loader = URLClassLoader.newInstance(
                    new URL[]{jarLoc},
                    getClass().getClassLoader()
            );

            this.pellet = (Pellet) Utils.deserialize(p, loader);

            this.pellet.setup(null, new PelletContext(pelletInstanceId));
        } catch (MalformedURLException e) {
            e.printStackTrace();
            LOGGER.error("Invalid Jar URL Exception: {}", e);
        }
    }*/


    /**
     * Pellet execution thread.
     * This is responsible for receiving messages from the backend
     */
    @Override
    public final void run() {
        final ZMQ.Socket signalReceiver = context.socket(ZMQ.SUB);
        signalReceiver.connect(
                Utils.Constants.FLAKE_RECEIVER_SIGNAL_BACKEND_SOCK_PREFIX
                + flakeId);

        //Receive messages meant for all.
        signalReceiver.subscribe(Utils.Constants.PUB_ALL.getBytes());

        //Receive messages meant only for this pellet.
        signalReceiver.subscribe(pelletInstanceId.getBytes());

        LOGGER.info("Open back channel from pellet");
//        final ZMQ.Socket backendBackChannel = context.socket(ZMQ.PUB);
//        backendBackChannel.connect(
//                Utils.Constants.FLAKE_BACKCHANNEL_SENDER_PREFIX
//                        + flakeId);

        //Create the emitter.
        if (pellet instanceof EmitterEnvelopeHook) {
            emitter = new MessageEmitter(flakeId, pelletName,
                    context, tupleSerializer, (EmitterEnvelopeHook) pellet);
        } else {
            emitter = new MessageEmitter(flakeId, pelletName,
                    context, tupleSerializer, null);
        }

        //Dummy execute with null values. NO NEED TO DO THIS HERE.
        //Fix for ISSUE #17. Changing this to start on a container signal.
        //pellet.execute(null, emitter);

        ZMQ.Poller pollerItems = new ZMQ.Poller(2);
        //pollerItems.register(dataReceiver, ZMQ.Poller.POLLIN);
        pollerItems.register(signalReceiver, ZMQ.Poller.POLLIN);

        Meter msgDequeuedMeter =  metricRegistry.meter(
                MetricRegistry.name(PelletExecutor.class, "dequed"));

        Meter msgProcessedMeter =  metricRegistry.meter(
                MetricRegistry.name(PelletExecutor.class, "processed"));

        //Timer queueTimer = metricRegistry.timer(
        //        MetricRegistry.name(PelletExecutor.class, "queue.latency"));

        //Timer processTimer = metricRegistry.timer(
        //        MetricRegistry.name(PelletExecutor.class, "process.latency"));

        Counter queLen = metricRegistry.counter(
                MetricRegistry.name(MsgReceiverComponent.class, "queue.len"));

        boolean disconnected = false;

        while (!Thread.currentThread().isInterrupted()) {
            LOGGER.debug("POLLING: ");
            //try {
                pollerItems.poll();

                if (pollerItems.pollin(1)) {
                    /*synchronized (started) {
                        if (!started) {
                            LOGGER.error("{}",pelletName);
                            continue;
                        }
                    }*/
                    this.pellet.execute(tupleIterator,
                            emitter,
                            pelletStateManager);
                } else if (pollerItems.pollin(0)) {
                    String envelope = signalReceiver
                            .recvStr(Charset.defaultCharset());
                    byte[] serializedSignal = signalReceiver.recv();
                    PelletSignal signal = (PelletSignal) Utils.deserialize(
                            serializedSignal);

                    if (signal instanceof SystemSignal) {
                        processSystemSignal((SystemSignal) signal, pollerItems,
                                dataReceiver);
                    } else {
                        if (pellet instanceof Signallable) {
                            ((Signallable) pellet).onSignal(signal);
                        } else {
                            LOGGER.warn("Pellet is not signallable.");
                        }
                    }
                }

                if (killSignalReceived && !disconnected) {
                    LOGGER.info("backlog: {}", dataReceiver.getBacklog());
                    //Stop receiving new data messages.
                    dataReceiver.disconnect(
                            Utils.Constants.FLAKE_RECEIVER_BACKEND_SOCK_PREFIX
                                    + flakeId);
                    //stop receiving new control messages?
                    signalReceiver.disconnect(
                            Utils.Constants
                                    .FLAKE_RECEIVER_SIGNAL_BACKEND_SOCK_PREFIX
                                    + flakeId);
                    disconnected = true;
                }

                //CURRENTLY NO CHECK IS DONE TO VERIFY IF ALL MESSAGES IN THE
                // QUEUE ARE PROCESSED AND SENT. THIS SHOULD BE DONE TO ENSURE
                // NO MESSAGE LOSS DURING SCALE IN.
                if (killSignalReceived && disconnected) {
                    break;
                }
           // } catch (Exception ex) {
           //     LOGGER.error("Error occured while executing pellet: {}", ex);
           // }
        }

        LOGGER.warn("Pellet executor stopped.");
        dataReceiver.close();
        signalReceiver.close();
        //backendBackChannel.close();
        //Runtime.getRuntime().removeShutdownHook(shutdownHook);
    }

    /**
     * processes the system signal for the pellet.
     * @param signal system signal.
     * @param pollerItems poller items to added to on start.
     * @param receiver data receiver socket to connect to in order to
     */
    private void processSystemSignal(final SystemSignal signal,
                                     final ZMQ.Poller pollerItems,
                                     final ZMQ.Socket receiver) {
        LOGGER.warn("System signal received: ");
        switch (signal.getSystemSignalType()) {
            case SwitchAlternate:
                LOGGER.warn("Switching pellet alternate.");
                this.pellet = (IteratorPellet) Utils.deserialize(
                                                signal.getSignalData(),
                                                loader);
                //this.pellet.setup(null, new PelletContext(pelletInstanceId));
                break;
            case StartInstance:
                LOGGER.info("Starting pellets.");

                this.pellet.onStart(appContext, pelletContext);

                //FIXME.. THIS SHOULD NOT BE REQUIRED.
                //PelletState state = getPelletState(null);
                //this.pellet.execute(null, emitter, state);
                pollerItems.register(receiver);
                if (pellet.getConf().isSourcePellet()) {
                    this.pellet.execute(tupleIterator, emitter,
                                                pelletStateManager);
                }
                synchronized (started) {
                    started = true;
                }
                LOGGER.error("Started:{}", pelletName);
                break;
            case KillInstance:
                LOGGER.warn("Kill Instance signal received. Terminating "
                        + "thread and closing connections?");
                killSignalReceived = true;
                break;
            default:
                LOGGER.warn("Unknown signal command.");
        }
    }

    /**
     * @return pellet instance's flake-unique index.
     */
    public final int getPelletInstanceIndex() {
        return pelletInstanceIndex;
    }

    /**
     * @return pellet instance's unique id
     * (flakeid + "-" + pelletInstanceIndex).
     */
    public final String getPelletInstanceId() {
        return pelletInstanceId;
    }
}

/*** POLL 0 backup in run
 dataReceiver.recvStr(Charset.defaultCharset());
 /*String sentTime
 = dataReceiver.recvStr(Charset.defaultCharset());*

 byte[] serializedTuple = dataReceiver.recv();

 queLen.dec();

 //long queueAddedTimeL = Long.parseLong(queueAddedTime);
 //long queueRemovedTime = System.nanoTime();
 //queueTimer.update(queueRemovedTime - queueAddedTimeL
 //        , TimeUnit.NANOSECONDS);

 msgDequeuedMeter.mark();

 Tuple tuple = tupleSerializer.deserialize(serializedTuple);


 //Run pellet.execute here.
 PelletState state = null;
 try {
 state = getPelletState(tuple);
 } catch (Exception ex) {
 LOGGER.error("Exception on T:{}", ex);
 }
 pellet.execute(tuple, emitter, state);
 /*if (state != null) {
 state.setLatestTimeStampAtomic(
 Long.parseLong(sentTime));
 }*


 long processedTime = System.nanoTime();
 //processTimer.update(processedTime - queueRemovedTime,
 //        TimeUnit.NANOSECONDS);
 msgProcessedMeter.mark();*/
