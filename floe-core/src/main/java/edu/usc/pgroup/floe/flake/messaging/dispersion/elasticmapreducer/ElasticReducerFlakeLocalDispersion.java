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

import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.flake.messaging.dispersion
        .FlakeLocalDispersionStrategy;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
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
     * Default constructor.
     */
    public ElasticReducerFlakeLocalDispersion() {

    }
    /**
     * Constructor.
     * @param metricRegistry Metrics registry used to log various metrics.
     * @param context       shared ZMQ context.
     * @param flakeId       Current flake id.
     *
    public ElasticReducerFlakeLocalDispersion(final MetricRegistry
                                                      metricRegistry,
                                              final ZMQ.Context context,
                                              final String flakeId) {
        super(metricRegistry, context, flakeId);
    }*/

    /**
     * Sends the tuple to the appropriate pellet.
     * @param from the middleend socket to retrieve the message
     * @param to the backend (PUB) socket to send it to (one or more) pellets.
     *
    @Override
    public final void sendToPellets(final ZMQ.Socket from,
                                    final ZMQ.Socket to) {
        String hashInt = from.recvStr(Charset.defaultCharset());
        LOGGER.info("HASH RECEIVED:{}", hashInt);
        Integer actualHash = Integer.parseInt(hashInt);
        String peinstanceid = getTargetPelletInstances(actualHash);
        if (peinstanceid != null) {
            to.sendMore(peinstanceid);
            Utils.forwardCompleteMessage(from, to);
        } else {
            Utils.recvAndignore(from);
        }
    }*/

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     *
     * @param tuple tuple object.
     * @param args custom arguments sent by the source flake with the tuple.
     * @return the list of target instances to send the given tuple.
     */
    @Override
    public final String getTargetPelletInstance(
            final Tuple tuple,
            final List<String> args) {
        String hashInt = args.get(0);
        LOGGER.info("HASH RECEIVED:{}", hashInt);
        Integer actualHash = Integer.parseInt(hashInt);
        return getTargetPelletInstances(actualHash);
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
     * List of target pellet instances.
     */
    private List<String> targetPelletIds;

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
     * @param args the arguments sent by the user. Fix Me: make this a better
     *             interface.
     */
    @Override
    public final void initialize(
            final String args) {
        this.targetPelletIds = new ArrayList<>();
        this.circle = new TreeMap<>(Collections.reverseOrder());
        this.reverseMap = new HashMap<>();
        this.hashingFunction = new Murmur32();
    }

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     * @param actualHash the actual hash of the tuple key.
     * @return the list of target instances to send the given tuple.
     */
    public final String getTargetPelletInstances(final Integer actualHash) {
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

        LOGGER.debug("LOCAL Key:{}, actualHash:{}, token:{}, target:{}",
                actualHash, hash, circle.get(hash));

        return circle.get(hash);
    }

    /**
     * @return the current backchannel data (e.g. for loadbalancing or the
     * token on the ring etc.)
     *
    @Override
    public final byte[] getCurrentBackchannelData() {
        LOGGER.debug("Token: {}", getToken());
        return Utils.serialize(getToken());
    }*/

    /**
     * Called whenever a new pellet is added.
     *
     * @param pelletId pellet instance id which has been added.
     */
    @Override
    public final void pelletAdded(final String pelletId) {
        LOGGER.info("Adding :{} to circle", pelletId);
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
