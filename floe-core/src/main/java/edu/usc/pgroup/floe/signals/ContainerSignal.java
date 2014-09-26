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

package edu.usc.pgroup.floe.signals;

import java.io.Serializable;

/**
 * @author kumbhare
 */
public final class ContainerSignal extends Signal implements Serializable {

    /**
     * List of container signals that can be sent to the container.
     * Currently this is not extensible and does not support.
     * Adding new signals without updating this class.
     */
    public enum ContainerSignalType {
         /**
         * Singal to start creating/updating flakes for a given application
         * on the container.
         */
        CREATE_FLAKES,
        /**
         * Signal to initiate the data and control channels.
         */
        CONNECT_FLAKES,
        /**
         * Signal to start launching pellets in the flake.
         */
        LAUNCH_PELLETS,
        /**
         * STOP ALL PELLETS.
         */
        STOP_PELLETS,
        /**
         * Signal to terminate all flakes. (for the given app)
         */
        TERMINATE_FLAKES,
        /**
         * Signal to start running pellets.
         */
        START_PELLETS
    }

    /**
     * The destination application name.
     */
    private final String destApp;

    /**
     * Destination pellet name.
     */
    private final String destContainer;

    /**
     * The type of the signal to send to the container.
     */
    private final ContainerSignalType signalType;

    /**
     * Constructor.
     * @param app destination application name.
     * @param type ContainerSingal Type. Currently this is not extensible and
     * does not support adding new signals without updating this class.
     * @param container destination container name.
     * @param data serialized signal data.
     */
    public ContainerSignal(final String app,
                           final ContainerSignalType type,
                           final String container,
                           final byte[] data) {
        super(data);
        this.signalType = type;
        this.destApp = app;
        this.destContainer = container;
    }

    /**
     * @return destination app name.
     */
    public String getDestApp() {
        return destApp;
    }

    /**
     * @return destination pellet name.
     */
    public String getDestContainer() { return destContainer; }

    /**
     * @return the type of the signal.
     */
    public ContainerSignalType getSignalType() { return signalType; }
}
