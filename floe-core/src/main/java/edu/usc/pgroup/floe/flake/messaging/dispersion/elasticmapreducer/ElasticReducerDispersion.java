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

package edu.usc.pgroup.floe.flake.messaging.dispersion.elasticmapreducer;

import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.flake.FlakeToken;
import edu.usc.pgroup.floe.flake.ZKFlakeTokenCache;
import edu.usc.pgroup.floe.flake.messaging.dispersion.MessageDispersionStrategy;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author kumbhare
 */
public class ElasticReducerDispersion extends MessageDispersionStrategy {

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
    private HashMap<String, Integer> flakeIdToTokenMap;

    /**
     * Key field name to be used for grouping.
     */
    private String keyFieldName;


    /**
     * List of target pellet instances.
     */
    private List<String> targetFlakeIds;

    /**
     * List of additional arguments to be sent.
     */
    private List<String> flakeArgs;

    /**
     * Temp. storage to store mapping from flake id to hash value.
     */
    private HashMap<String, Integer> actualHashes;

    /**
     * Hash function to be used.
     */
    private HashingFunction hashingFunction;

    /**
     * Path cache to monitor the tokens.
     */
    private ZKFlakeTokenCache flakeCache;

    /**
     * @param appName    Application name.
     * @param pelletName dest pellet name to be used to get data from ZK.
     * @param myFlakeId Current flake's id.
     */
    public ElasticReducerDispersion(final String appName,
                                    final String pelletName,
                                    final String myFlakeId) {
        super(appName, pelletName, myFlakeId);
        this.targetFlakeIds = new ArrayList<>();
        this.flakeArgs = new ArrayList<>();
        this.circle = new TreeMap<>(Collections.reverseOrder());
        this.flakeIdToTokenMap = new HashMap<>();
        this.actualHashes = new HashMap<>();
        this.hashingFunction = new Murmur32();
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
        String key = middleendreceiver.recvStr(Charset.defaultCharset());

        byte[] seralized = null;
        if (key instanceof  String) {
            seralized = ((String) key).getBytes();
        } else {
            LOGGER.info("KEY IS NOT STRING. Use of string keys is suggested.");
            seralized = Utils.serialize(key);
        }
        Integer actualHash = hashingFunction.hash(seralized);




        Integer hash = getTargetFlakeHash(actualHash);
        String fid =  circle.get(hash);

        LOGGER.debug("Sending to:{}", hash.toString());
        backend.sendMore(fid.toString());
        backend.sendMore(actualHash.toString());
        backend.sendMore(String.valueOf(System.currentTimeMillis()));
        Utils.forwardCompleteMessage(middleendreceiver, backend);
    }*/

    /**
     * @param args the arguments sent by the user. Fix Me: make this a better
     *             interface.
     */
    @Override
    protected final void initialize(final String args) {
        this.keyFieldName = args;
    }

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     * param tuple tuple object.
     * @param tuple incoming tuple
     * @return the list of target instances to send the given tuple
     */
    @Override
    public final List<String> getTargetFlakeIds(final Tuple tuple) {
        Object key = tuple.get(keyFieldName);

        byte[] seralized = null;
        if (key instanceof  String) {
            seralized = ((String) key).getBytes();
        } else {
            LOGGER.info("KEY IS NOT STRING. Use of string keys is suggested.");
            seralized = Utils.serialize(key);
        }
        Integer actualHash = hashingFunction.hash(seralized);
        Integer hash = getTargetFlakeHash(actualHash);
        String fid =  circle.get(hash);

        actualHashes.put(fid, actualHash);

        targetFlakeIds.clear();
        targetFlakeIds.add(fid);
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
        flakeArgs.clear();
        flakeArgs.add(actualHashes.get(fId).toString());
        flakeArgs.add(String.valueOf(System.currentTimeMillis()));
        return flakeArgs;
    }

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     * @param actualHash the actual hash of the field.
     * @return the list of target instances to send the given tuple.
     */
    public final synchronized Integer getTargetFlakeHash(
            final Integer actualHash) {
        if (circle.isEmpty()) {
            return null;
        }

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
                actualHash, hash, circle.get(hash));

        return hash;

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
    }

    /**
     * Updates the circle.
     * @param targetFlakeId flake id from which the message is received.
     * @param newPosition position of the target flake on the ring.
     * @param toContinue true if the flake is sending a regular backchannel
     *                   msg. False if the message is sent on scaling down i
     *                   .e. 'terminate' is called on the target flake.
     */
    public final synchronized void updateCircle(final String targetFlakeId,
                                   final Integer newPosition,
                                   final boolean toContinue) {
        Integer current = flakeIdToTokenMap.get(targetFlakeId);
        if (current != null) {
            circle.remove(current);
            flakeIdToTokenMap.remove(targetFlakeId);
        }

        if (toContinue) {
            LOGGER.info("received Token: {} for {}",
                    newPosition, targetFlakeId);

            flakeIdToTokenMap.put(targetFlakeId, newPosition);

            String prev = circle.get(newPosition);

            if (prev != null) {
                circle.remove(newPosition);
                flakeIdToTokenMap.remove(prev);
            }

            circle.put(newPosition, targetFlakeId);


        }

        LOGGER.error("Circle:{}", circle);
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
            updateCircle(flake.getFlakeID(), flake.getToken(), true);
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
        updateCircle(token.getFlakeID(), token.getToken(), true);
    }

    /**
     * This function is called whenever a flake is removed for the
     * correspondong pellet.
     *
     * @param token flake token corresponding to the added flake.
     */
    @Override
    public final void flakeRemoved(final FlakeToken token) {
        updateCircle(token.getFlakeID(), token.getToken(), false);
    }

    /**
     * This function is called whenever a data associated with a flake
     * corresponding to the given pellet is updated.
     *
     * @param token updated flake token.
     */
    @Override
    public final void flakeDataUpdated(final FlakeToken token) {
        updateCircle(token.getFlakeID(), token.getToken(), true);
    }
}
