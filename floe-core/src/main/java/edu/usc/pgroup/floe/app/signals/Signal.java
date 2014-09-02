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

package edu.usc.pgroup.floe.app.signals;

import java.io.Serializable;

/**
 * @author kumbhare
 */
public class Signal implements Serializable {

    /**
     * The destination application name.
     */
    private final String destApp;

    /**
     * Destination pellet name.
     */
    private final String destPellet;


    /**
     * Signal signalData.
     */
    private final byte[] signalData;


    /**
     * Constructor.
     * @param app destination application name.
     * @param pellet destination pellet name.
     * @param data serialized signal data.
     */
    public Signal(final String app,
                  final String pellet,
                  final byte[] data) {
        this.destApp = app;
        this.destPellet = pellet;
        this.signalData = data;
    }

    /**
     * @return the signal signalData.
     */
    public final byte[] getSignalData() {
        return signalData;
    }

    /**
     * @return destination app name.
     */
    public final String getDestApp() {
        return destApp;
    }

    /**
     * @return destination pellet name.
     */
    public final String getDestPellet() {
        return destPellet;
    }

    /**
     * Signal type enum.
     */
    public enum SignalType {
        /**
         * System type signal. Used by flake to send a command to ALL the
         * pellet instances running on that flake.
         */
        SystemSignal,
        /**
         * User type signal. Signals that are sent by the user. By default
         * the signal is considered as user type.
         */
        UserSignal
    }
}