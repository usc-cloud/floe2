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
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.ZKPaths;
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
     * Path cache to monitor the tokens.
     */
    private PathChildrenCache flakeCache;

    /**
     * Initializes the strategy.
     * @param appName Application name.
     * @param destPelletName dest pellet name to be used to get data from ZK.
     * @param args the arguments sent by the user. Fix Me: make this a better
     *             interface.
     */
    @Override
    public final void initialize(
            final String appName,
            final String destPelletName,
            final String args) {
        this.targetFlakeIds = new ArrayList<>();
        this.circle = new TreeMap<>();
        this.reverseMap = new HashMap<>();
        this.keyFieldName = args;
        this.hashingFunction = new Murmur32();

        String pelletTokenPath = ZKUtils.getApplicationPelletTokenPath(
                appName, destPelletName);
        LOGGER.info("Listening for flake tokens for dest pellet: {} at {}",
                destPelletName, pelletTokenPath);
        this.flakeCache = new PathChildrenCache(ZKClient.getInstance()
                .getCuratorClient(), pelletTokenPath, true);

        try {
            flakeCache.start();
            flakeCache.rebuild();
            List<ChildData> childData = flakeCache.getCurrentData();
            for (ChildData child: childData) {
                String destFid = ZKPaths.getNodeFromPath(child.getPath());
                LOGGER.info("Dest FID: {}", destFid);
                Integer newPosition = (Integer) Utils.deserialize(
                                                    child.getData());
                updateCircle(destFid, newPosition, true);
                //targetFlakeIds.add(destFid);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.warn("Error occured while retreving flake information for "
                    + "destination pellet,");
        }
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
            targetFlakeIds.add(circle.get(neighborToken));
        }

        Iterator<Integer> frontIterator = circle.keySet().iterator();
        for (; i < replication && frontIterator.hasNext(); i++) {
            Integer neighborToken = frontIterator.next();
            targetFlakeIds.add(circle.get(neighborToken));
        }*/

        return targetFlakeIds;
    }

    /**
     * Call back whenever a message is received from a target pellet instance
     * on the back channel. This can be used by dispersion strategy to choose
     * the target instance to send the message to.
     *  @param targetFlakeId flake id from which the
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
        Integer newPosition = (Integer) Utils.deserialize(message);
        updateCircle(targetFlakeId, newPosition, toContinue);

        LOGGER.debug("Circle: {}", circle);
    }

    /**
     * Updates the circle.
     * @param targetFlakeId flake id from which the message is received.
     * @param newPosition position of the target flake on the ring.
     * @param toContinue true if the flake is sending a regular backchannel
     *                   msg. False if the message is sent on scaling down i
     *                   .e. 'terminate' is called on the target flake.
     */
    public final void updateCircle(final String targetFlakeId,
                                   final Integer newPosition,
                                   final boolean toContinue) {
        Integer current = reverseMap.get(targetFlakeId);
        if (current != null) {
            circle.remove(current);
        }

        if (toContinue) {


            LOGGER.debug("received Token: {}", newPosition);

            reverseMap.put(targetFlakeId, newPosition);
            circle.put(newPosition, targetFlakeId);
        }
    }
}
