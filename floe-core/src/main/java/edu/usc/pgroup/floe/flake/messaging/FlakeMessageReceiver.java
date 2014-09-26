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

import edu.usc.pgroup.floe.container.FlakeControlCommand;
import edu.usc.pgroup.floe.flake.Flake;
import edu.usc.pgroup.floe.signals.SystemSignal;
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
     * Once the poller.poll returns, use this function as a component in the
     * proxy to forward messages from one socket to another.
     * @param from socket to read from.
     * @param to socket to send messages to
     */
    private void forwardCompleteMessage(final ZMQ.Socket from,
                                        final ZMQ.Socket to) {
        byte[] message;
        boolean more = false;
        while (true) {
            // receive message
            message = from.recv(0);
            more = from.hasReceiveMore();
            // Broker it
            to.send(message, more ? ZMQ.SNDMORE : 0);
            if (!more) {
                break;
            }
        }
    }

    /**
     * This is used to start the proxy from tcp socket to the pellets.
     */
    public final void run() {

        //Frontend socket to talk to other flakes. dont connect here. Connect
        // only when the signal for connect is received.
        LOGGER.info("Starting front end receiver socket");
        ZMQ.Socket frontend = ctx.socket(ZMQ.XSUB);

        //Backend socket to talk to the Pellets contained in the flake. The
        // pellets may be added or removed dynamically.
        LOGGER.info("Starting backend inproc socket to communicate with "
                + "pellets at: "
                + Utils.Constants.FLAKE_RECEIVER_BACKEND_SOCK_PREFIX
                + flake.getFlakeId());
        ZMQ.Socket backend = ctx.socket(ZMQ.XPUB);
        backend.bind(Utils.Constants.FLAKE_RECEIVER_BACKEND_SOCK_PREFIX
                + flake.getFlakeId());

        LOGGER.info("Starting inproc socket to send signals to pellets: "
                + Utils.Constants.FLAKE_RECEIVER_SIGNAL_BACKEND_SOCK_PREFIX
                + flake.getFlakeId());
        ZMQ.Socket signal = ctx.socket(ZMQ.PUB);
        signal.bind(Utils.Constants.FLAKE_RECEIVER_SIGNAL_BACKEND_SOCK_PREFIX
                + flake.getFlakeId());

        LOGGER.info("Starting backend ipc socket for control channel at: "
                + Utils.Constants.FLAKE_RECEIVER_CONTROL_SOCK_PREFIX
                + flake.getFlakeId());
        ZMQ.Socket controlSocket = ctx.socket(ZMQ.REP);
        controlSocket.bind(Utils.Constants.FLAKE_RECEIVER_CONTROL_SOCK_PREFIX
                + flake.getFlakeId());

        ZMQ.Socket killsock  = ctx.socket(ZMQ.SUB);
        killsock.connect(Utils.Constants.FLAKE_KILL_CONTROL_SOCK_PREFIX
                        + flake.getFlakeId());
        killsock.subscribe(Utils.Constants.PUB_ALL.getBytes());

        //XPUB XSUB sockets for the backchannels.
        ZMQ.Socket xsubFromPelletsSock = ctx.socket(ZMQ.XSUB);
        LOGGER.info("WAITING FOR BACKCHANNEL CONNECTINON "
                + "FROM PELLET EXECUTOR. {}", flake.getFlakeId());
        xsubFromPelletsSock.bind(
                Utils.Constants.FLAKE_BACKCHANNEL_PELLET_PROXY_PREFIX
                        + flake.getFlakeId());
        //connect to the back channel on connect signal.
        ZMQ.Socket xpubToPredSock = ctx.socket(ZMQ.XPUB);

        ZMQ.Poller pollerItems = new ZMQ.Poller(6);
        pollerItems.register(frontend, ZMQ.Poller.POLLIN);
        pollerItems.register(controlSocket, ZMQ.Poller.POLLIN);
        pollerItems.register(killsock, ZMQ.Poller.POLLIN);
        pollerItems.register(xsubFromPelletsSock, ZMQ.Poller.POLLIN);
        pollerItems.register(xpubToPredSock, ZMQ.Poller.POLLIN);
        pollerItems.register(backend, ZMQ.Poller.POLLIN);

        byte[] message;
        boolean more = false;
        while (!Thread.currentThread().isInterrupted()) {
            pollerItems.poll();
            if (pollerItems.pollin(0)) { //frontend
                forwardCompleteMessage(frontend, backend);
            } else if (pollerItems.pollin(5)) { //backend
                forwardCompleteMessage(backend, frontend);
            } else if (pollerItems.pollin(1)) { //controlSocket
                message = controlSocket.recv();

                byte[] result = new byte[]{'1'};

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
                        break;
                    case DISCONNECT_PRED:
                        String disconnectstr = (String) command.getData();
                        LOGGER.info("disconnecting from: " + disconnectstr);
                        frontend.disconnect(disconnectstr);
                        break;
                    case PELLET_SIGNAL:
                        LOGGER.info("Received signal for: "
                                + flake.getFlakeId());
                        signal.sendMore(Utils.Constants.PUB_ALL);
                        signal.send((byte[]) command.getData(), 0);
                        break;
                    case SWITCH_ALTERNATE:
                        LOGGER.info("Switching alternate for: "
                                + flake.getFlakeId());
                        SystemSignal systemSignal = new SystemSignal(
                                flake.getAppName(),
                                flake.getPelletId(),
                                SystemSignal.SystemSignalType.SwitchAlternate,
                                (byte[]) command.getData()
                        );
                        signal.sendMore(Utils.Constants.PUB_ALL);
                        signal.send(Utils.serialize(systemSignal), 0);
                        break;
                    default:
                        result = flake.processControlSignal(command, signal);
                }
                controlSocket.send(result, 0);
            } else if (pollerItems.pollin(2)) { //kill socket
                break;
            } else if (pollerItems.pollin(3)) { //from xsubFromPelletsSock
                forwardCompleteMessage(xsubFromPelletsSock, xpubToPredSock);
            } else if (pollerItems.pollin(4)) { //from xpubToPredSock
                forwardCompleteMessage(xpubToPredSock, xsubFromPelletsSock);
            }
        }
        LOGGER.warn("Closing flake receiver sockets");

        frontend.close();
        controlSocket.close();
        killsock.close();
        xsubFromPelletsSock.close();
        xpubToPredSock.close();
        backend.close();
    }
}
