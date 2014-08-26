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

package edu.usc.pgroup.floe.messaging;

import edu.usc.pgroup.floe.container.FlakeControlCommand;
import edu.usc.pgroup.floe.flake.Flake;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * The receiver end of the flake.
 * @author kumbhare
 */
public class FlakeMessageReceiver extends Thread {


    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FlakeMessageReceiver.class);

    /**
     * The ZMQ context.
     */
    private final ZMQ.Context ctx;

    /**
     * the flake instance to which this receiver belongs.
     */
    private final Flake flake;

    /**
     * constructor.
     * @param context shared ZMQ context (this is required for inproc://)
     * @param f The flake instance to which this receiver is bound.
     */
    public FlakeMessageReceiver(final ZMQ.Context context,
                                final Flake f) {
        this.flake = f;
        this.ctx = context;
    }

    /**
     * This is used to start the proxy from tcp socket to the pellets.
     */
    public final void run() {

        //Frontend socket to talk to other flakes.
        LOGGER.info("Starting front end receiver socket");
        ZMQ.Socket frontend = ctx.socket(ZMQ.PULL);


        //WE MUST CHANGE THIS. DO NOT CONNECT HERE. CONNECT SHOULD HAPPEN ON
        // CONTROL SIGNAL.
        //frontend.connect("tcp://" + predecessorIPorHostName + ":" + port);

        //Backend socket to talk to the Pellets contained in the flake. The
        // pellets may be added or removed dynamically.
        LOGGER.info("Starting backend inproc socket to communicate with "
                + "pellets at: "
                + Utils.Constants.FLAKE_RECEIVER_BACKEND_SOCK_PREFIX
                + flake.getId());
        ZMQ.Socket backend = ctx.socket(ZMQ.PUSH);
        backend.bind(Utils.Constants.FLAKE_RECEIVER_BACKEND_SOCK_PREFIX
                + flake.getId());

        LOGGER.info("Starting backend ipc socket for control channel at: "
                + Utils.Constants.FLAKE_RECEIVER_CONTROL_SOCK_PREFIX
                + flake.getId());
        ZMQ.Socket controlSocket = ctx.socket(ZMQ.REP);
        controlSocket.bind(Utils.Constants.FLAKE_RECEIVER_CONTROL_SOCK_PREFIX
                + flake.getId());

        ZMQ.Poller pollerItems = new ZMQ.Poller(2);
        pollerItems.register(frontend, ZMQ.Poller.POLLIN);
        pollerItems.register(controlSocket, ZMQ.Poller.POLLIN);

        while (!Thread.currentThread().isInterrupted()) {
            byte[] message;
            LOGGER.debug("POLLING: ");
            pollerItems.poll();
            if (pollerItems.pollin(0)) {
                //forward to the backend.
                message = frontend.recv();
                backend.send(message, 0);
            } else if (pollerItems.pollin(1)) {
                message = controlSocket.recv();
                //process control message.
                FlakeControlCommand command
                        = (FlakeControlCommand) Utils.deserialize(
                        message);
                        /*FlakeCommandExecutor.execute(command);*/
                LOGGER.warn("Received command: " + command);
                switch (command.getCommand()) {
                    case CONNECT_PRED:
                        String connectstr = (String) command.getData();
                        LOGGER.info("Connecting to: " + connectstr);
                        frontend.connect(connectstr);
                        break;
                    case DISCONNECT_PRED:
                        String disconnectstr = (String) command.getData();
                        LOGGER.info("disconnecting from: " + disconnectstr);
                        frontend.disconnect(disconnectstr);
                        break;
                    case INCREMENT_PELLET:
                        byte[] bpellet = (byte[]) command.getData();

                        LOGGER.info("CREATING PELLET: on " + flake.getId());
                        flake.incrementPellet(bpellet);
                        break;
                    case DECREMENT_PELLET:
                        String dpid = (String) command.getData();
                        LOGGER.info("REMOVING PELLET: " + dpid + " on "
                                + flake.getId());
                        //flake.incrementPellet();
                        break;
                    default:
                        LOGGER.warn("Unrecognized command: " + command);
                        break;
                }

                //TODO: Get valid results here. Must define a results format.
                byte[] result = new byte[]{'a'};
                controlSocket.send(result, 0);
            }
        }

        //Route all incoming messages "equally" among the various pellets.
        //THIS PROXY WONT WORK. WE WILL HAVE TO DO CUSTOM HERE. SINCE
        //1. WE CANNOT USE THE SOCKET OBJECT IN DIFFERENT THREAD.
        //2. WE WANT TO RECEIVE CONTROL SIGNALS TO SCALE UP/SCALE DOWN.
        ZMQ.proxy(frontend, backend, null);

        ctx.close();
    }
}
