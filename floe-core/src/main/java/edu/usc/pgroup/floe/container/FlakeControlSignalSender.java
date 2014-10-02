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

package edu.usc.pgroup.floe.container;

import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.HashMap;
import java.util.Map;

/**
 * @author kumbhare
 */
public final class FlakeControlSignalSender {
    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FlakeControlSignalSender.class);

    /**
     * ZMQ Context.
     */
    private final ZMQ.Context zcontext;

    /**
     * Map of flake ids to ZMQ sender.
     */
    private Map<String, ZMQWrapper> senderMap;

    /**
     * the singleton instance.
     */
    private static FlakeControlSignalSender instance;

    /**
     * Hiding the private constructor.
     */
    private FlakeControlSignalSender() {
        senderMap = new HashMap<>();
        zcontext = ZMQ.context(1);
    }

    /**
     * @return the singleton sender instance.
     */
    public static synchronized FlakeControlSignalSender getInstance() {
        if (instance == null) {
            instance = new FlakeControlSignalSender();
        }

        return instance;
    }

    /**
     * To send the command to the flake identified by flakeId.
     * @param flakeId id of the flake.
     * @param command command to send
     */
    public synchronized void send(final String flakeId,
                              final FlakeControlCommand command) {
        if (!senderMap.containsKey(flakeId)) {
            ZMQWrapper wrapper = new ZMQWrapper(flakeId);
            senderMap.put(flakeId, wrapper);
        }

        ZMQWrapper wrapper = senderMap.get(flakeId);
        wrapper.send(command);
    }

    /**
     * Internal class for the ZMQ sender.
     */
    class ZMQWrapper {

        /**
         * Flake id.
         */

        private final String id;
        /**
         * ZMQ socket client for the sender.
         */
        private ZMQ.Socket sender;

        /**
         * Constructor.
         * @param flakeId Id of the flake to communicate with.
         */
        public ZMQWrapper(final String flakeId) {
            this.id = flakeId;
            this.sender = zcontext.socket(ZMQ.REQ);
            LOGGER.info("Connecting to control channel at: "
                    + Utils.Constants.FLAKE_CONTROL_SOCK_PREFIX
                    + flakeId);
            sender.bind(Utils.Constants.FLAKE_CONTROL_SOCK_PREFIX
                    + flakeId);

            Thread shutdownHook = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        LOGGER.info("Closing flake contorl sock.");
                        sender.close();
                    }
                });
            Runtime.getRuntime().addShutdownHook(shutdownHook);
        }

        /**
         * Send the command to the corresponding flake.
         * @param command command to send
         */
        public final void send(final FlakeControlCommand command) {
            byte[] data = Utils.serialize(command);
            LOGGER.info("Sending command: {} to flake {}", command, id);
            sender.send(data, 0);
            byte[] results = sender.recv();
            LOGGER.info("Received data: {} from flake", results[0]);
        }
    }
}
