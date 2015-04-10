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

package edu.usc.pgroup.floe.flake.messaging.dispersion.roundrobin;

import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.flake.FlakeToken;
import edu.usc.pgroup.floe.flake.messaging.dispersion.MessageDispersionStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kumbhare
 */
public class RRDispersionStrategy extends MessageDispersionStrategy {


    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RRDispersionStrategy.class);

    /**
     * Current index in the RR strategy.
     */
    private int currentIndex;

    /**
    * List of target pellet instances.
    */
    private List<String> allTargetFlakes;

    /**
     * List of target pellet instances.
     */
    private List<String> targetFlakeIds;

    /**
     * @param appName    Application name.
     * @param pelletName dest pellet name to be used to get data from ZK.
     * @param myFlakeId Current flake's id.
     */
    public RRDispersionStrategy(final String appName,
                                final String pelletName,
                                final String myFlakeId) {
        super(appName, pelletName, myFlakeId);
        targetFlakeIds = new ArrayList<>();
        allTargetFlakes = new ArrayList<>();
        currentIndex = 0;
    }


    /**
      * @param args the arguments sent by the user. Fix Me: make this a better
      *             interface.
      */
     @Override
     protected final void initialize(final String args) {

     }

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     * param tuple tuple object.
     * return the list of target instances to send the given tuple.
     *
     * @param middleendreceiver middleend receiver to get the message.
     * @param backend backend sender to send message to the succeeding flakes.
     *
    @Override
    public final void disperseMessage(final ZMQ.Socket middleendreceiver,
                                      final ZMQ.Socket backend) {
        backend.sendMore(getNextFlakeId());
        backend.sendMore(String.valueOf(System.currentTimeMillis()));
        Utils.forwardCompleteMessage(middleendreceiver, backend);
    }*/


    /**
    * Returns the list of target instances to send the given tuple using the
    * defined strategy.
    * param tuple tuple object.
    * @param tuple tuple to be dispersed
    * @return the list of target instances to send the given tuple.
    */
    @Override
    public final List<String> getTargetFlakeIds(final Tuple tuple) {
        targetFlakeIds.clear();
        targetFlakeIds.add(getNextFlakeId());
        return targetFlakeIds;
    }

    /**
     * Should return a list of arguments/"envelopes" to be sent along with
     * the message for the given target flake.
     *
     * @param fId one of the flake ids returned by getTargetFlakeIds
     * @return list of arguments to be sent.
     */
    @Override
    public final List<String> getCustomArguments(final String fId) {
        return null;
    }

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     * @return the list of target instances to send the given tuple.
     */
    public final String getNextFlakeId() {
        if (currentIndex >= allTargetFlakes.size()) {
            currentIndex = 0;
        }

        if (allTargetFlakes.size() == 0) {
            return null;
        }

        String fid = allTargetFlakes.get(currentIndex);
        currentIndex++;
        return fid;
    }

    /**
     * This function is called exactly once when the initial flake list is
     * fetched.
     *
     * @param flakes list of currently initialized flakes.
     */
    @Override
    public final void initialFlakeList(final List<FlakeToken> flakes) {
        for (FlakeToken flake: flakes) {
            allTargetFlakes.add(flake.getFlakeID());
        }
    }

    /**
     * This function is called whenever a new flake is created for the
     * correspondong pellet.
     *
     * @param token flake token corresponding to the added flake.
     */
    @Override
    public final void flakeAdded(final FlakeToken token) {
        if (!allTargetFlakes.contains(token.getFlakeID())) {
            allTargetFlakes.add(token.getFlakeID());
        }
    }

    /**
     * This function is called whenever a flake is removed for the
     * correspondong pellet.
     *
     * @param token flake token corresponding to the added flake.
     */
    @Override
    public final void flakeRemoved(final FlakeToken token) {
        allTargetFlakes.remove(token.getFlakeID());
    }

    /**
     * This function is called whenever a data associated with a flake
     * corresponding to the given pellet is updated.
     *
     * @param token updated flake token.
     */
    @Override
    public final void flakeDataUpdated(final FlakeToken token) {

    }
}
