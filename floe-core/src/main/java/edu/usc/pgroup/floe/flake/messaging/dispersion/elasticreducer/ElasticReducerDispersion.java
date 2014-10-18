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
import edu.usc.pgroup.floe.flake.FlakeToken;
import edu.usc.pgroup.floe.flake.ZKFlakeTokenCache;
import edu.usc.pgroup.floe.flake.messaging.dispersion.MessageDispersionStrategy;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import edu.usc.pgroup.floe.zookeeper.zkcache.PathChildrenUpdateListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author kumbhare
 */
public class ElasticReducerDispersion implements MessageDispersionStrategy,
        PathChildrenUpdateListener {

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
     * Hash function to be used.
     */
    private HashingFunction hashingFunction;

    /**
     * Path cache to monitor the tokens.
     */
    private ZKFlakeTokenCache flakeCache;

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
        this.flakeIdToTokenMap = new HashMap<>();
        this.keyFieldName = args;
        this.hashingFunction = new Murmur32();

        String pelletTokenPath = ZKUtils.getApplicationPelletTokenPath(
                appName, destPelletName);
        LOGGER.debug("Listening for flake tokens for dest pellet: {} at {}",
                destPelletName, pelletTokenPath);
        this.flakeCache = new ZKFlakeTokenCache(pelletTokenPath, this);

                //new PathChildrenCache(ZKClient.getInstance()
                //.getCuratorClient(), pelletTokenPath, true);


        try {
            //flakeCache.start();
            //flakeCache.rebuild();
            flakeCache.rebuild();
            List<ChildData> childData = flakeCache.getCurrentCachedData();
            for (ChildData child: childData) {
                String destFid = ZKPaths.getNodeFromPath(child.getPath());
                LOGGER.warn("Dest FID: {}", destFid);

                /*Integer newPosition = (Integer) Utils.deserialize(
                                                    child.getData());*/
                FlakeToken token = (FlakeToken) Utils.deserialize(
                                                        child.getData());

                updateCircle(destFid, token.getToken(), true);
                //targetFlakeIds.add(destFid);
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.warn("Error occured while retreving flake information for "
                    + "destination pellet: {}", e);
        }
    }

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     * param tuple tuple object.
     * return the list of target instances to send the given tuple.
     *
     * @param middleendreceiver
     * @param backend
     */
    @Override
    public void disperseMessage(ZMQ.Socket middleendreceiver,
                                ZMQ.Socket backend) {
        String key = middleendreceiver.recvStr(Charset.defaultCharset());

        byte[] seralized = null;
        if (key instanceof  String) {
            seralized = ((String) key).getBytes();
        } else {
            LOGGER.info("KEY IS NOT STRING. Use of string keys is suggested.");
            seralized = Utils.serialize(key);
        }
        Integer actualHash = hashingFunction.hash(seralized);

        LOGGER.info("KEY: {}, ACTUAL HASH SENDING:{}", key, actualHash);
        Integer hash = getTargetFlakeHash(actualHash);
        String fid =  circle.get(hash);


        LOGGER.debug("Sending to:{}", hash.toString());
        backend.sendMore(fid.toString());
        backend.sendMore(actualHash.toString());
        backend.sendMore(String.valueOf(System.currentTimeMillis()));
        Utils.forwardCompleteMessage(middleendreceiver, backend);
    }

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     *
     * @return the list of target instances to send the given tuple.
     */
    public synchronized final Integer getTargetFlakeHash(
            Integer actualHash) {
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
        //Integer newPosition = (Integer) Utils.deserialize(message);
        //updateCircle(targetFlakeId, newPosition, toContinue);

        //LOGGER.debug("Circle: {}", circle);
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
        }

        if (toContinue) {
            LOGGER.debug("received Token: {}", newPosition);

            flakeIdToTokenMap.put(targetFlakeId, newPosition);
            circle.put(newPosition, targetFlakeId);
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

        FlakeToken token = (FlakeToken) Utils.deserialize(
                addedChild.getData());

        updateCircle(destFid, token.getToken(), true);
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

        FlakeToken token = (FlakeToken) Utils.deserialize(
                removedChild.getData());

        updateCircle(destFid, token.getToken(), false);
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
        String destFid = ZKPaths.getNodeFromPath(updatedChild.getPath());
        LOGGER.error("Updating dest FID: {}", destFid);

        FlakeToken token = (FlakeToken) Utils.deserialize(
                updatedChild.getData());

        updateCircle(destFid, token.getToken(), true);
    }
}
