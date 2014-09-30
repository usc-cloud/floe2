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

package edu.usc.pgroup.floe.flake.messaging.dispersion.elasticreducer;

import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.flake.messaging.dispersion.MessageDispersionStrategy;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author kumbhare
 */
public class ElasticReducerDispersion implements MessageDispersionStrategy {
    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ElasticReducerDispersion.class);

    /**
     * The hash ring for consistent hashing.
     */
    private SortedMap<Integer, String> circle;

    /**
     * Reverse map which stores the mapping from the flakeid to its current
     * value.
     */
    private HashMap<String, Integer> reverseMap;

    /**
     * Key field name to be used for grouping.
     */
    private String keyFieldName;


    /**
     * List of target pellet instances.
     */
    private List<String> targetFlakeIds;

    /**
     * Hash function to be used.
     */
    private HashingFunction hashingFunction;

    /**
     * Initializes the strategy.
     *
     * @param args the arguments sent by the user. Fix Me: make this a better
     *             interface.
     */
    @Override
    public final void initialize(final String args) {
        this.targetFlakeIds = new ArrayList<>();
        this.circle = new TreeMap<>();
        this.reverseMap = new HashMap<>();
        this.keyFieldName = args;
        this.hashingFunction = new Murmur32();
    }

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     *
     * @param tuple tuple object.
     * @return the list of target instances to send the given tuple.
     */
    @Override
    public final List<String> getTargetFlakeIds(final Tuple tuple) {
        if (circle.isEmpty()) {
            return null;
        }
        Object key = tuple.get(keyFieldName);
        byte[] seralized = null;
        if (key instanceof  String) {
            seralized = ((String) key).getBytes();
        } else {
            LOGGER.info("KEY IS NOT STRING. Use of string keys is suggested.");
            seralized = Utils.serialize(key);
        }
        int actualHash = hashingFunction.hash(seralized);
        int hash = actualHash;
        if (!circle.containsKey(hash)) {
            SortedMap<Integer, String> tailMap = circle.tailMap(hash);

            if (tailMap.isEmpty()) {
                hash = circle.firstKey();
            } else {
                hash = tailMap.firstKey();
            }
        }
        LOGGER.debug("Key:{}, actualHash:{}, token:{}, target:{}",
                key, actualHash, hash, circle.get(hash));
        targetFlakeIds.clear();
        targetFlakeIds.add(circle.get(hash));
        return targetFlakeIds;
    }

    /**
     * Call back whenever a message is received from a target pellet instance
     * on the back channel. This can be used by dispersion strategy to choose
     * the target instance to send the message to.
     *  @param targetFlakeId pellet instance id from which the
     *                      message is received.
     * @param message       message body.
     * @param toContinue true if the flake is sending a regular backchannel
     *                   msg. False if the message is sent on scaling down i
     *                   .e. 'terminate' is called on the target flake.
     */
    @Override
    public final void backChannelMessageReceived(final String targetFlakeId,
                                                 final byte[] message,
                                                 final Boolean toContinue) {

        Integer current = reverseMap.get(targetFlakeId);
        if (current != null) {
            circle.remove(current);
        }

        if (toContinue) {
            Integer newPosition = (Integer) Utils.deserialize(message);

            LOGGER.debug("received Token: {}", newPosition);

            reverseMap.put(targetFlakeId, newPosition);
            circle.put(newPosition, targetFlakeId);
        }
        LOGGER.debug("Circle: {}", circle);
    }
}
