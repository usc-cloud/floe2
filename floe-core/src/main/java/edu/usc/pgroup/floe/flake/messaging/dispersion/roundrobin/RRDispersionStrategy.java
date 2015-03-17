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
import edu.usc.pgroup.floe.flake.messaging.dispersion.MessageDispersionStrategy;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
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
     * Default constructor.
     */
    public RRDispersionStrategy() {
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
     * @param flakeId one of the flake ids returned by getTargetFlakeIds
     * @return list of arguments to be sent.
     */
    @Override
    public final List<String> getCustomArguments(final String flakeId) {
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
        if (!allTargetFlakes.contains(targetFlakeId)) {
            allTargetFlakes.add(targetFlakeId);
        }
    }



    /**
     * Triggered when initial list of children is cached.
     * This is retrieved synchronously.
     *
     * @param initialChildren initial list of children.
     */
    @Override
    public final void childrenListInitialized(
            final Collection<ChildData> initialChildren) {
        for (ChildData child: initialChildren) {
            String destFid = ZKPaths.getNodeFromPath(child.getPath());
            LOGGER.warn("Dest FID: {}", destFid);
            allTargetFlakes.add(destFid);
        }
    }

    /**
     * Triggered when a new child is added.
     * Note: this is not recursive.
     *
     * @param addedChild newly added child's data.
     */
    @Override
    public final void childAdded(final ChildData addedChild) {
        String destFid = ZKPaths.getNodeFromPath(addedChild.getPath());
        LOGGER.error("Adding Dest FID: {}", destFid);
        allTargetFlakes.add(destFid);
    }

    /**
     * Triggered when an existing child is removed.
     * Note: this is not recursive.
     *
     * @param removedChild removed child's data.
     */
    @Override
    public final void childRemoved(final ChildData removedChild) {
        String destFid = ZKPaths.getNodeFromPath(removedChild.getPath());
        LOGGER.error("Removing dest FID: {}", destFid);
        allTargetFlakes.remove(destFid);
    }

    /**
     * Triggered when a child is updated.
     * Note: This is called only when Children data is also cached in
     * addition to stat information.
     *
     * @param updatedChild update child's data.
     */
    @Override
    public final void childUpdated(final ChildData updatedChild) {
        //ignore token value changes in RR.
    }
}
