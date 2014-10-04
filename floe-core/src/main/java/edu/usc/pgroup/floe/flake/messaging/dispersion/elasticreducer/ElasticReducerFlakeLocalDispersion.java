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
import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.flake.messaging
        .dispersion.FlakeLocalDispersionStrategy;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author kumbhare
 */
public class ElasticReducerFlakeLocalDispersion
        extends FlakeLocalDispersionStrategy {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ElasticReducerFlakeLocalDispersion.class);

    /**
     * Constructor.
     *
     * @param srcPelletName The name of the src pellet on this edge.
     * @param context       shared ZMQ context.
     * @param flakeId       Current flake id.
     * @param token Flake's token on the ring.
     */
    public ElasticReducerFlakeLocalDispersion(final String srcPelletName,
                                              final ZMQ.Context context,
                                              final String flakeId,
                                              final Integer token) {
        super(srcPelletName, context, flakeId, token);
    }

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
    private List<String> targetPelletIds;

    /**
     * Hash function to be used.
     */
    private HashingFunction hashingFunction;

    /**
     * Replication factor to be used.
     */
    private int replication;


    /**
     * Initializes the strategy.
     * @param args the arguments sent by the user. Fix Me: make this a better
     *             interface.
     */
    @Override
    public final void initialize(final String args) {
        this.targetPelletIds = new ArrayList<>();
        this.circle = new TreeMap<>(Collections.reverseOrder());
        this.reverseMap = new HashMap<>();
        this.keyFieldName = args;
        this.hashingFunction = new Murmur32();
        this.replication = FloeConfig.getConfig().getInt(
                ConfigProperties.FLAKE_TOLERANCE_LEVEL);
    }

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     *
     * @param tuple tuple object.
     * @return the list of target instances to send the given tuple.
     */
    @Override
    public final List<String> getTargetPelletInstances(final Tuple tuple) {
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
            SortedMap<Integer, String> headMap = circle.headMap(hash);

            if (headMap.isEmpty()) {
                hash = circle.lastKey();
            } else {
                hash = headMap.lastKey();
            }
        }

        LOGGER.debug("LOCAL Key:{}, actualHash:{}, token:{}, target:{}",
                key, actualHash, hash, circle.get(hash));
        targetPelletIds.clear();

        targetPelletIds.add(circle.get(hash));

        /** NOT REQUIRED............... SINCE FLAKES KNOW ABOUT THEIR
         * NEIGHBOURS. EACH NEIGHBOUR CAN JUST AD AN EXTRA SUBSCRIPTION.
         * THAT WAY WE CAN TAKE ADVANTAGE OF THE MULTI CAST PROTOCOL EASILY.
         */
        //Add backups.. for PEER MESSAGE BACKUP
        /*SortedMap<Integer, String> tail = circle.tailMap(hash);
        Iterator<Integer> iterator = tail.keySet().iterator();
        iterator.next(); //ignore the self's token.

        int i = 0;
        for (; i < replication && iterator.hasNext(); i++) {
            Integer neighborToken = iterator.next();
            targetPelletIds.add(circle.get(neighborToken));
        }

        Iterator<Integer> frontIterator = circle.keySet().iterator();
        for (; i < replication && frontIterator.hasNext(); i++) {
            Integer neighborToken = frontIterator.next();
            targetPelletIds.add(circle.get(neighborToken));
        }*/

        return targetPelletIds;
    }

    /**
     * @return the current backchannel data (e.g. for loadbalancing or the
     * token on the ring etc.)
     */
    @Override
    public final byte[] getCurrentBackchannelData() {
        LOGGER.debug("MyToken: {}", getToken());
        return Utils.serialize(getToken());
    }

    /**
     * Called whenever a new pellet is added.
     *
     * @param pelletId pellet instance id which has been added.
     */
    @Override
    public final void pelletAdded(final String pelletId) {
        Integer pelletPosition = hashingFunction.hash(pelletId.getBytes());

        reverseMap.put(pelletId, pelletPosition);
        circle.put(pelletPosition, pelletId);
        LOGGER.info("Added. Circle: {}", circle);
    }

    /**
     * Called whenever a pellet is removed.
     *
     * @param pelletId pellet instance id which has been added.
     */
    @Override
    public final void pelletRemoved(final String pelletId) {
        Integer key = reverseMap.get(pelletId);
        circle.remove(key);
        LOGGER.info("Removed. Circle: {}", circle);
    }
}
