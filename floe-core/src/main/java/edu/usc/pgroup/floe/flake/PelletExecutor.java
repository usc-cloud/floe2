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

import edu.usc.pgroup.floe.app.Pellet;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.app.signals.Signal;
import edu.usc.pgroup.floe.app.signals.Signallable;
import edu.usc.pgroup.floe.messaging.MessageEmitter;
import edu.usc.pgroup.floe.serialization.SerializerFactory;
import edu.usc.pgroup.floe.serialization.TupleSerializer;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;

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
     * The fully qualified pellet class name.
     */
    private String pelletClass;


    /**
     * Instance of the pellet class.
     */
    private Pellet pellet;

    /**
     * Serializer to be used to serialize and deserialize the data tuples.
     */
    private final TupleSerializer tupleSerializer;

    /**
     * hiding default constructor.
     * @param sharedContext shared ZMQ context to be used in inproc comm. for
     *                      receiving message from the flake.
     * @param fid flake's id to which this pellet belongs.
     */
    private PelletExecutor(final ZMQ.Context sharedContext, final String fid) {
        this.context = sharedContext;
        this.tupleSerializer = SerializerFactory.getSerializer();
        this.flakeId = fid;
    }

    /**
     * Construct pellet instance from fqdn.
     * @param fqdnClass the fully qualified class name for the pellet.
     * @param sharedContext shared ZMQ context to be used in inproc comm. for
     *                      receiving message from the flake.
     * @param fid flake's id to which this pellet belongs.
     */
    PelletExecutor(final String fqdnClass, final String fid,
                   final ZMQ.Context sharedContext) {
        this(sharedContext, fid);
        this.pelletClass = fqdnClass;
        this.pellet = (Pellet) Utils.instantiateObject(pelletClass);
    }



    /**
     * Construct pellet instance from de-serialized version.
     * @param p pellet instance from the user.
     * @param sharedContext shared ZMQ context to be used in inproc comm. for
     *                      receiving message from the flake.
     * @param fid flake's id to which this pellet belongs.
     */
    public PelletExecutor(final Pellet p,  final String fid,
                          final ZMQ.Context sharedContext) {
        this(sharedContext, fid);
        this.pellet = p;
    }

    /**
     *
     * @param p serialized pellet
     * @param app application name
     * @param appJar application's jar file namne.
     * @param fid flake id
     * @param sharedContext shared zmq contex.
     */
    public PelletExecutor(final byte[] p,
                          final String app,
                          final String appJar,
                          final String fid,
                          final ZMQ.Context sharedContext) {

        this(sharedContext, fid);
        try {
            File relativeJarLoc = new File(
                    Utils.getContainerJarDownloadPath(app, appJar));

            URL jarLoc = new URL(
                    "file://" + relativeJarLoc.getAbsolutePath());

            LOGGER.info("Loading jar: {} into class loader.", jarLoc);
            URLClassLoader loader = URLClassLoader.newInstance(
                    new URL[]{jarLoc},
                    getClass().getClassLoader()
            );

            this.pellet = (Pellet) Utils.deserialize(p, loader);
        } catch (MalformedURLException e) {
            e.printStackTrace();
            LOGGER.error("Invalid Jar URL Exception: {}", e);
        }


    }


    /**
     * Pellet execution thread.
     * This is responsible for receiving messages from the backend
     */
    @Override
    public final void run() {
        ZMQ.Socket dataReceiver = context.socket(ZMQ.PULL);
        dataReceiver.connect(Utils.Constants.FLAKE_RECEIVER_BACKEND_SOCK_PREFIX
                + flakeId);

        ZMQ.Socket signalReceiver = context.socket(ZMQ.SUB);
        signalReceiver.connect(
                Utils.Constants.FLAKE_RECEIVER_SIGNAL_BACKEND_SOCK_PREFIX
                + flakeId);
        signalReceiver.subscribe("".getBytes()); //dummy topic.

        //Create the emitter.
        MessageEmitter emitter = new MessageEmitter(flakeId,
                        context, tupleSerializer);

        //Dummy execute with null values.
        pellet.execute(null, emitter);

        ZMQ.Poller pollerItems = new ZMQ.Poller(2);
        pollerItems.register(dataReceiver, ZMQ.Poller.POLLIN);
        pollerItems.register(signalReceiver, ZMQ.Poller.POLLIN);

        while (!Thread.currentThread().isInterrupted()) {
            byte[] message;
            LOGGER.debug("POLLING: ");
            pollerItems.poll();
            if (pollerItems.pollin(0)) {
                byte[] serializedTuple = dataReceiver.recv();
                Tuple tuple = tupleSerializer.deserialize(serializedTuple);
                //Run pellet.execute here.
                pellet.execute(tuple, emitter);
            } else if (pollerItems.pollin(1)) {
                byte[] serializedSignal = signalReceiver.recv();
                Signal signal = (Signal) Utils.deserialize(serializedSignal);
                //Run pellet.execute here.
                if (pellet instanceof Signallable) {
                    ((Signallable) pellet).onSignal(signal);
                } else {
                    LOGGER.warn("Pellet is not signallable.");
                }
            }

        }

        dataReceiver.close();
        signalReceiver.close();
        //context.destroy();
    }
}
