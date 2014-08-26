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

package edu.usc.pgroup.floe.coordinator;

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.thriftgen.TCoordinator;
import org.apache.commons.configuration.Configuration;
import org.apache.thrift.TProcessor;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Alok Kumbhare
 */
public final class CoordinatorService {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CoordinatorService.class);

    /**
     * Hiding default constructor.
     */
    private CoordinatorService() {

    }

    /**
     * Main function.
     *
     * @param args commandline arguments.
     */
    public static void main(final String[] args) {
        initializeCoordinator();
        startThriftServer();
    }

    /**
     * Initializes the Coordinator which will handle all internal coordination
     * and monitoring tasks.
     * CoordinatorHandler on the other hand is responsible for receiving user
     * commands from the client.
     */
    private static void initializeCoordinator() {
        LOGGER.info("Initializing coordinator");
        Coordinator.getInstance(); //This will initialize the coordinator.
    }

    /**
     * starts the server.
     */
    private static void startThriftServer() {
        //Start the server
        LOGGER.info("Starting thrift server.");
        try {

            Configuration config = FloeConfig.getConfig();

            //Transport
            int port = config.getInt(ConfigProperties.COORDINATOR_PORT);
            LOGGER.info("Listening on port: " + port);
            TNonblockingServerTransport serverSocket =
                    new TNonblockingServerSocket(port);

            //Processor
            TProcessor processor =
                    new TCoordinator.Processor(new CoordinatorHandler());

            //Arguments
            THsHaServer.Args args = new THsHaServer.Args(serverSocket);
            args.workerThreads(config.getInt(
                    ConfigProperties.COORDINATOR_SERVICE_THREAD_COUNT));
            args.processor(processor);

            //Server
            TServer server = new THsHaServer(args);

            //Serve
            server.serve();

        } catch (Exception e) {
            LOGGER.error("Exception: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
