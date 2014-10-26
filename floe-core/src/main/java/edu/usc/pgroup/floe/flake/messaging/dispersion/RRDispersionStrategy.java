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

import edu.usc.pgroup.floe.utils.Utils;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kumbhare
 */
public class RRDispersionStrategy implements MessageDispersionStrategy {

    /**
     * Current index in the RR strategy.
     */
    private int currentIndex;


    /**
     * List of target pellet instances.
     */
    private List<String> targetFlakeIds;

    /**
     * Initializes the strategy.
     * @param appName Application name.
     * @param destPelletName dest pellet name to be used to get data from ZK.
     * @param args the arguments sent by the user. Fix Me: make this a better
     *             interface.
     */
    @Override
    public final void initialize(final String appName,
                                  final String destPelletName,
                                  final String args) {
        targetFlakeIds = new ArrayList<>();
        currentIndex = 0;
    }

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     * param tuple tuple object.
     * return the list of target instances to send the given tuple.
     *
     * @param middleendreceiver middleend receiver to get the message.
     * @param backend backend sender to send message to the succeeding flakes.
     */
    @Override
    public final void disperseMessage(final ZMQ.Socket middleendreceiver,
                                      final ZMQ.Socket backend) {
        backend.sendMore(getNextFlakeId());
        Utils.forwardCompleteMessage(middleendreceiver, backend);
    }

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     * @return the list of target instances to send the given tuple.
     */
    public final String getNextFlakeId() {
        if (currentIndex >= targetFlakeIds.size()) {
            currentIndex = 0;
        }

        if (targetFlakeIds.size() == 0) {
            return null;
        }

        String fid = targetFlakeIds.get(currentIndex);
        currentIndex++;
        return fid;
    }

    /**
     * Call back whenever a message is received from a target pellet instance
     * on the back channel. This can be used by dispersion strategy to choose
     * the target instance to send the message to.
     *  @param targetFlakeId pellet instance id from which the
     *                               message is received.
     * @param message                message body.
     * @param toContinue true if the flake is sending a regular backchannel
     *                   msg. False if the message is sent on scaling down i
     *                   .e. 'terminate' is called on the target flake.
     */
    @Override
    public final void backChannelMessageReceived(
            final String targetFlakeId,
            final byte[] message, final Boolean toContinue) {
        if (!targetFlakeIds.contains(targetFlakeId)) {
            targetFlakeIds.add(targetFlakeId);
        }
    }
}
