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

package edu.usc.pgroup.floe.flake.messaging.dispersion;

import edu.usc.pgroup.floe.app.Tuple;

import java.util.List;

/**
 * @author kumbhare
 */
public interface FlakeLocalDispersionStrategy extends PelletUpdateListener {
    /**
     * Initializes the strategy.
     * @param args the arguments sent by the user. Fix Me: make this a better
     *             interface.
     */
    void initialize(String args);

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     * @param tuple tuple object.
     * @return the list of target instances to send the given tuple.
     */
    List<String> getTargetPelletInstances(Tuple tuple);
}

