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

import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * Each of the different components of the flake such as MessageReceiver,
 * StateManager, PelletExecutor, MessageSender and so on.
 * @author kumbhare
 */
public abstract class FlakeComponent {
    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FlakeComponent.class);


    /**
     * Flake's id to which this component belongs.
     */
    private final String fid;

    /**
     * Unique name of the component.
     */
    private final String name;

    /**
     * Shared ZMQ context.
     */
    private final ZMQ.Context context;

    /**
     * Listenes for start/stop notifications from the underlying component.
     */
    private final ZMQ.Socket notifyListenerSock;

    /**
     * Used to send the kill signal to the component.
     */
    private final ZMQ.Socket killSignalSender;

    /**
     * Constructor.
     * @param flakeId Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx Shared zmq context.
     */
    public FlakeComponent(final String flakeId,
                          final String componentName,
                          final ZMQ.Context ctx) {
        this.fid = flakeId;
        this.name = componentName;
        this.context = ctx;

        this.notifyListenerSock = context.socket(ZMQ.PULL);
        this.notifyListenerSock.bind(Utils.Constants.
                FLAKE_COMPONENT_NOTIFY_PREFIX + fid + name);

        this.killSignalSender = context.socket(ZMQ.PUSH);
        this.killSignalSender.bind(Utils.Constants.
                FLAKE_COMPONENT_KILL_PREFIX + fid + name);
    }

    /**
     * Starts the given component and waits for it to be started.
     * @return true if the component was started successfully, false otherwise
     */
    public final synchronized boolean startAndWait() {
        Thread componentRunner = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        ZMQ.Socket terminateSignalReceiver
                                = context.socket(ZMQ.PULL);
                        //connect here.
                        terminateSignalReceiver.connect(Utils.Constants.
                                FLAKE_COMPONENT_KILL_PREFIX + fid + name);
                        runComponent(terminateSignalReceiver);
                        terminateSignalReceiver.close();
                    }
                }
        );
        LOGGER.info("Starting component: {}", name);
        componentRunner.start();
        LOGGER.info("Waiting for started notification for {}", name);
        byte[] result = notifyListenerSock.recv(); //wait
        LOGGER.info("{} started with status {}", name, result);
        if (result[0] == 1) {
            return true;
        }
        return false;
    }

    /**
     * Starts the given component and waits for it to be started.
     * @return true if the component was started successfully, false otherwise
     */
    public final synchronized boolean stopAndWait() {
        LOGGER.info("Stopping component: {}", name);
        killSignalSender.send(new byte[]{1}, 0);

        LOGGER.info("Waiting for Stopped notification for {}", name);
        byte[] result = notifyListenerSock.recv(); //wait

        LOGGER.info("{} Stopped with status {}", name, result);
        if (result[0] == 1) {
            return true;
        }
        return false;
    }


    /**
     * Sends a notify signal on the notify socket.
     * @param status the status of the request operation. true is successful.
     *               false otherwise.
     */
    private void notify(final boolean status) {
        ZMQ.Socket socket = context.socket(ZMQ.PUSH);
        socket.connect(Utils.Constants.
                FLAKE_COMPONENT_NOTIFY_PREFIX + fid + name);

        byte[] result = new byte[1];

        if (status) {
            result[0] = 1;
        } else {
            result[0] = 0;
        }

        socket.send(result, 0);
    }

    /**
     * Notifies the startAndWait function that the component has started.
     * @param status the status parameter indicating whether the component
     *               started successfully or not.
     */
    protected final void notifyStarted(final boolean status) {
        notify(status);
    }

    /**
     * Notifies the stopAndWait function that the component has started.
     * @param status the status parameter indicating whether the component
     *               started successfully or not.
     */
    protected final void notifyStopped(final boolean status) {
        notify(status);
    }

    /**
     * Starts all the sub parts of the given component and notifies when
     * components starts completely. This will be in a different thread,
     * so no need to worry.. block as much as you want.
     * @param terminateSignalReceiver terminate signal receiver.
     */
    protected abstract void runComponent(ZMQ.Socket terminateSignalReceiver);

    /**
     * @return the shared zmq context.
     */
    protected final ZMQ.Context getContext() {
        return context;
    }

    /**
     * @return flake's unique id.
     */
    protected final String getFid() {
        return fid;
    }
}
