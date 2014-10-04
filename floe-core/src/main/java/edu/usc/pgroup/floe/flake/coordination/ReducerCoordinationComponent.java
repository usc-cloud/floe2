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

package edu.usc.pgroup.floe.flake.coordination;

import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author kumbhare
 */
public class ReducerCoordinationComponent extends CoordinationComponent {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ZKUtils.class);

    /**
     * level of tolerance. (i.e. number of flake
     *                       failures to tolerate).
     */
    private final Integer toleranceLevel;

    /**
     * Fixme. Should not be string.
     * K = toleranceLevel Neighbor flakes in counter clockwise direction.
     */
    private final SortedMap<Integer, String> neighbors;

    /**
     * Flake's current token on the ring.
     */
    private final Integer myToken;

    /**
     * Path cache to monitor the tokens.
     */
    private PathChildrenCache flakeCache;

    /**
     * Constructor.
     *
     * @param app           the application name.
     * @param pellet        pellet's name to which this flake belongs.
     * @param flakeId       Flake's id to which this component belongs.
     * @param token       This flake's current token value.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     * @param tolerance level of tolerance. (i.e. number of flake
     *                       failures to tolerate).
     */
    public ReducerCoordinationComponent(final String app,
                                        final String pellet,
                                        final String flakeId,
                                        final Integer token,
                                        final String componentName,
                                        final ZMQ.Context ctx,
                                        final Integer tolerance) {
        super(app, pellet, flakeId, componentName, ctx);
        this.toleranceLevel = tolerance;
        this.neighbors = new TreeMap<>();
        this.myToken = token;
    }

    /**
     * Starts all the sub parts of the given component and notifies when
     * components starts completely. This will be in a different thread,
     * so no need to worry.. block as much as you want.
     *
     * @param terminateSignalReceiver terminate signal receiver.
     */
    @Override
    protected final void runComponent(
            final ZMQ.Socket terminateSignalReceiver) {
        String pelletTokenPath = ZKUtils.getApplicationPelletTokenPath(
                getAppName(), getPelletName());

        flakeCache = new PathChildrenCache(ZKClient.getInstance()
                .getCuratorClient(), pelletTokenPath, true);

        boolean success = true;
        try {
            flakeCache.start();
            flakeCache.rebuild();


            List<ChildData> childData = flakeCache.getCurrentData();

            extractNeighbours(childData);

        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Could not start token monitor.");
            success = false;
        }

        notifyStarted(success);

        terminateSignalReceiver.recv();

        success = true;
        try {
            flakeCache.close();
        } catch (IOException e) {
            e.printStackTrace();
            success = false;
        }
        notifyStopped(success);
    }

    /**
     * Finds k neighbor flakes in counter clockwise direction.
     * @param childData data received from ZK cache.
     */
    private void extractNeighbours(final List<ChildData> childData) {
        SortedMap<Integer, String> allFlakes = new TreeMap<>(
                Collections.reverseOrder());

        for (ChildData child: childData) {
            String path = child.getPath();
            Integer data = (Integer) Utils.deserialize(child.getData());
            allFlakes.put(data, path);
            LOGGER.info("CHILDREN: {} , TOKEN: {}", path, data);
        }

        SortedMap<Integer, String> tail = allFlakes.tailMap(myToken);
        Iterator<Integer> iterator = tail.keySet().iterator();
        iterator.next(); //ignore the self's token.

        int i = 0;
        for (; i < toleranceLevel && iterator.hasNext(); i++) {
            Integer neighborToken = iterator.next();
            neighbors.put(neighborToken, allFlakes.get(neighborToken));
        }

        Iterator<Integer> headIterator = allFlakes.keySet().iterator();
        for (; i < toleranceLevel && headIterator.hasNext(); i++) {
            Integer neighborToken = headIterator.next();
            neighbors.put(neighborToken, allFlakes.get(neighborToken));
        }

        LOGGER.info("ME:{}, Neighbors: {}", myToken, neighbors);
    }
}
