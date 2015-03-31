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
import edu.usc.pgroup.floe.flake.FlakesTracker;

import java.util.List;

/**
 * @author kumbhare
 */
public abstract class MessageDispersionStrategy extends FlakesTracker {

    /**
     *  Current flake's id.
     */
    private final String flakeId;

    /**
     * @param appName Application name.
     * @param pelletName dest pellet name to be used to get data from ZK.
     * @param myFlakeId Current flake's id.
     */
    public MessageDispersionStrategy(final String appName,
                                     final String pelletName,
                                     final String myFlakeId) {
        super(appName, pelletName);
        this.flakeId = myFlakeId;
        start();
    }

    /**
     * Initializes the strategy.
     * @param args the arguments sent by the user. Fix Me: make this a better
     *             interface.
     */
    protected abstract void initialize(String args);

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     * @param tuple tuple object.
     * @return the list of target instances to send the given tuple.
     */
    public abstract List<String> getTargetFlakeIds(Tuple tuple);


    /**
     * Should return a list of arguments/"envelopes" to be sent along with
     * the message for the given target flake.
     * @param flakeId one of the flake ids returned by getTargetFlakeIds
     * @return list of arguments to be sent.
     */
    public abstract List<String> getCustomArguments(String flakeId);

    /**
     *
     * @return Current flake's id.
     */
    public String getFlakeId() {
        return flakeId;
    }
}
