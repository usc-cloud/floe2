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
public class PelletSignal extends Signal implements Serializable {

    /**
     * The destination application name.
     */
    private final String destApp;

    /**
     * Destination pellet name.
     */
    private final String destPellet;


    /**
     * Constructor.
     * @param app destination application name.
     * @param pellet destination pellet name.
     * @param data serialized signal data.
     */
    public PelletSignal(final String app,
                        final String pellet,
                        final byte[] data) {
        super(data);
        this.destApp = app;
        this.destPellet = pellet;
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
}
