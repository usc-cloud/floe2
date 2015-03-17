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

import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.flake.FlakeToken;
import edu.usc.pgroup.floe.flake.ZKFlakeTokenCache;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import edu.usc.pgroup.floe.zookeeper.zkcache.PathChildrenUpdateListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author kumbhare
 */
public abstract class MessageDispersionStrategy implements BackChannelReceiver,
        PathChildrenUpdateListener {


    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(MessageDispersionStrategy.class);

    /**
     * Path cache to monitor the tokens.
     */
    private ZKFlakeTokenCache flakeCache;

    /**
     *
     * @param args the arguments sent by the user. Fix Me: make this a better
     *             interface.
     */
    protected abstract void initialize(String args);

    /**
     * Initializes the strategy.
     * @param appName Application name.
     * @param destPelletName dest pellet name to be used to get data from ZK.
     * @param args the arguments sent by the user. Fix Me: make this a better
     *             interface.
     */
    public final void initialize(final String appName,
                                 final String destPelletName,
                                 final String args) {
        initialize(args);

        String pelletTokenPath = ZKUtils.getApplicationPelletTokenPath(
                appName, destPelletName);
        LOGGER.debug("Listening for flake tokens for dest pellet: {} at {}",
                destPelletName, pelletTokenPath);
        this.flakeCache = new ZKFlakeTokenCache(pelletTokenPath, this);

        try {
            flakeCache.rebuild();
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
     */
    public abstract List<String> getTargetFlakeIds(Tuple tuple);


    /**
     * Should return a list of arguments/"envelopes" to be sent along with
     * the message for the given target flake.
     * @param flakeId one of the flake ids returned by getTargetFlakeIds
     * @return list of arguments to be sent.
     */
    public abstract List<String> getCustomArguments(String flakeId);

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     * param tuple tuple object.
     * return the list of target instances to send the given tuple.
     *
     * @param middleendreceiver middleend receiver to get the message.
     * @param backend backend sender to send message to the succeeding flakes.
     */
    //void disperseMessage(ZMQ.Socket middleendreceiver, ZMQ.Socket backend);
}
