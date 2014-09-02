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

package edu.usc.pgroup.floe.utils;

import edu.usc.pgroup.floe.app.signals.Signal;

/**
 * @author kumbhare
 */
public class SystemSignal extends Signal {

    /**
     * System signal type.
     */
    private final SystemSignalType systemSignalType;

    /**
     * Constructor.
     *
     * @param app    destination application name.
     * @param pellet destination pellet name.
     * @param sst    The System Signal type to be sent to the pellet instances.
     * @param data   serialized signal data.
     */
    public SystemSignal(final String app, final String pellet,
                        final SystemSignalType sst,
                        final byte[] data) {
        super(app, pellet, data);
        this.systemSignalType = sst;
    }

    /**
     * @return the system signal type.
     */
    public final SystemSignalType getSystemSignalType() {
        return systemSignalType;
    }

    /**
     * Enum for system signal types.
     */
    public enum SystemSignalType {

        /**
         * System signal sent to a pellet instance to kill itself.
         */
        KillInstance,

        /**
         * System signal for switching alternates.
         */
        SwitchAlternate
    }
}
