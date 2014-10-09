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

import java.io.Serializable;

/**
 * @author kumbhare
 */
public class FlakeToken implements Serializable {
    /**
     * the flake's current token on the ring.
     */
    private int token;
    /**
     * the ip or host name to be used to connect to this host.
     */
    private String ipOrHost;
    /**
     * the port to connect to the host.
     */
    private int stateCheckptPort;

    /**
     *
     * @param tk the int token for the flake on the ring.
     * @param ipOrHostName ip or hostname for the flake to connect to.
     * @param port the port number to connect to.
     */
    public FlakeToken(final int tk,
                      final String ipOrHostName,
                      final int port) {
        this.token = tk;
        this.ipOrHost = ipOrHostName;
        this.stateCheckptPort = port;
    }

    /**
     * @return the flake's current token on the ring.
     */
    public final int getToken() {
        return token;
    }

    /**
     * @return the ip or host name to be used to connect to this host.
     */
    public final String getIpOrHost() {
        return ipOrHost;
    }

    /**
     * @return the port to connect to the host.
     */
    public final int getStateCheckptPort() {
        return stateCheckptPort;
    }
}
