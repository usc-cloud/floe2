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

package edu.usc.pgroup.floe.app;

import edu.usc.pgroup.floe.signals.PelletSignal;

import java.util.List;

/**
 * @author kumbhare
 */
public abstract class BasePellet implements Pellet, Signallable {

    /**
     * @return The names of the streams to be used later during emitting
     * messages.
     */
    @Override
    public List<String> getOutputStreamNames() {
        return null;
    }

    /**
     * Called when a signal is received for the component.
     * @param signal the signal received for this pellet.
     */
    @Override
    public void onSignal(final PelletSignal signal) { }
}
