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

import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

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
     * List of ports to listen on.
     * Note: This is fine since the number of ports depends only on logical
     * application graph and NOT on pellet instances.
     */
    private final int[] ports;

    /**
     * constructor.
     * @param zmqContext Shared ZMQ context.
     * @param flakeId flake id to which this sender belongs.
     * @param listeningPorts list of ports to listen on for connections from the
     *              successor flakes. One per edge in the app graph.
     */
    public FlakeMessageSender(final ZMQ.Context zmqContext,
                              final String flakeId,
                              final int[] listeningPorts) {
        this.ctx = zmqContext;
        this.fid = flakeId;
        this.ports = listeningPorts;
    }


    /**
     * This is used to start the proxy from tcp socket to the pellets.
     */
    public final void run() {
        new MiddleEnd().start();
        for (int port: ports) {
            new BackEnd(port).start();
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
         * constructor.
         * @param p port on which the back end should listen for
         *             connections from downstream.
         */
        public BackEnd(final int p) {
            this.port = p;
        }

        /**
         * Backend thread's run method. This is responsible for the routing.
         */
        public void run() {

            ZMQ.Socket middleendreceiver  = ctx.socket(ZMQ.SUB);

            middleendreceiver.connect(
                    Utils.Constants.FLAKE_SENDER_MIDDLEEND_SOCK_PREFIX + fid);

            //dummy topic. THIS IS A PREFIX MATCH. USE THIS LATER FOR CUSTOM
            // DISPERSION STRATEGIES.
            middleendreceiver.subscribe("".getBytes());

            ZMQ.Socket killsock  = ctx.socket(ZMQ.SUB);
            killsock.connect(
                    Utils.Constants.FLAKE_KILL_CONTROL_SOCK_PREFIX
                            + fid);
            killsock.subscribe(Utils.Constants.PUB_ALL.getBytes());

            ZMQ.Socket backend  = ctx.socket(ZMQ.PUSH);
            backend.bind(
                  Utils.Constants.FLAKE_SENDER_BACKEND_SOCK_PREFIX
                    + port);

            ZMQ.Poller pollerItems = new ZMQ.Poller(2);
            pollerItems.register(middleendreceiver, ZMQ.Poller.POLLIN);
            pollerItems.register(killsock, ZMQ.Poller.POLLIN);

            while (!Thread.currentThread().isInterrupted()) {
                byte[] message;
                pollerItems.poll();
                if (pollerItems.pollin(0)) {
                    message = middleendreceiver.recv();
                    backend.send(message, 0);
                } else if (pollerItems.pollin(1)) {
                    break;
                }
            }
            //ZMQ.proxy(middleendreceiver, backend, null);
            LOGGER.warn("Closing flake backend sockets");
            middleendreceiver.close();
            backend.close();
            killsock.close();
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
            ZMQ.Socket frontend  = ctx.socket(ZMQ.PULL);
            frontend.bind(
                    Utils.Constants.FLAKE_SENDER_FRONTEND_SOCK_PREFIX
                            + fid);


            ZMQ.Socket middleend  = ctx.socket(ZMQ.PUB);
            middleend.bind(
                    Utils.Constants.FLAKE_SENDER_MIDDLEEND_SOCK_PREFIX
                            + fid);


            ZMQ.Socket killsock  = ctx.socket(ZMQ.SUB);
            killsock.connect(
                    Utils.Constants.FLAKE_KILL_CONTROL_SOCK_PREFIX
                            + fid);
            killsock.subscribe(Utils.Constants.PUB_ALL.getBytes());

            ZMQ.Poller pollerItems = new ZMQ.Poller(2);
            pollerItems.register(frontend, ZMQ.Poller.POLLIN);
            pollerItems.register(killsock, ZMQ.Poller.POLLIN);

            while (!Thread.currentThread().isInterrupted()) {
                byte[] message;
                pollerItems.poll();
                if (pollerItems.pollin(0)) {
                    message = frontend.recv();
                    middleend.send(message, 0);
                } else if (pollerItems.pollin(1)) {
                    break;
                }
            }
            LOGGER.warn("Closing flake middleend sockets");
            frontend.close();
            middleend.close();
            killsock.close();
        }
    }
}
