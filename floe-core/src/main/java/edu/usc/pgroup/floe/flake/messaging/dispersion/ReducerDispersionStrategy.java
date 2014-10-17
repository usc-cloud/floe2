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
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import javax.rmi.CORBA.Util;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * @author kumbhare
 */
public class ReducerDispersionStrategy implements MessageDispersionStrategy {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ReducerDispersionStrategy.class);

    /**
     * Key field name to be used for grouping.
     */
    private String keyFieldName;


    /**
     * List of target pellet instances.
     */
    private List<String> targetFlakeIds;

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
        this.keyFieldName = args;
        String pelletTokenPath = ZKUtils.getApplicationPelletTokenPath(
                appName, destPelletName);
        this.flakeCache = new PathChildrenCache(ZKClient.getInstance()
                .getCuratorClient(), pelletTokenPath, true);

        try {
            flakeCache.start();
            flakeCache.rebuild();
            List<ChildData> childData = flakeCache.getCurrentData();
            for (ChildData child: childData) {

                targetFlakeIds.add(ZKPaths.getNodeFromPath(child.getPath()));
            }
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.warn("Error occured while retreving flake information for "
                    + "destination pellet {}", e);
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
        String fid = getTargetFlakeIds(key);
        backend.sendMore(fid);
        backend.sendMore(key);
        Utils.forwardCompleteMessage(middleendreceiver, backend);
    }

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     *
     * @return the list of target instances to send the given tuple.
     */
    public final String getTargetFlakeIds(String key) {
        int currentIndex = key.hashCode() % targetFlakeIds.size();
        return targetFlakeIds.get(currentIndex);
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
