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

package edu.usc.pgroup.floe.client;

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base Floe client class, takes care of connecting to the Floe server.
 *
 * @author Alok Kumbhare
 */
public class ThriftClient {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ThriftClient.class);

    /**
     * the transport class (use TFramedTransport for THsHaServer).
     */
    private TTransport transport;

    /**
     * the protocol to be used for communication (binary, json etc.).
     */
    private TProtocol protocol;

    /**
     * default public constructor. Establishes connection with the Floe Server.
     *
     * @throws TTransportException Exception
     */
    public ThriftClient() throws TTransportException {
        String host = FloeConfig.getConfig().getString(
                ConfigProperties.COORDINATOR_HOST);

        int port = FloeConfig.getConfig().getInt(
                ConfigProperties.COORDINATOR_PORT);

        TTransport baseTransport = new TSocket(host, port);
        transport = new TFramedTransport(baseTransport);
        transport.open();
        LOGGER.info("Connection to Floe Server has been established.");
        if (transport != null) {
            protocol = new TBinaryProtocol(transport);
        }
    }

    /**
     * @return the protocol to use for communication.
     */
    public final TProtocol getProtocol() {
        return protocol;
    }
}
