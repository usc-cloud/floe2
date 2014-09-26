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

package edu.usc.pgroup.floe.signals;

import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.slf4j.LoggerFactory;


/**
 * @author kumbhare
 */
public final class SignalHandler {

    /**
     * the global logger instance.
     */
    private static final org.slf4j.Logger LOGGER =
            LoggerFactory.getLogger(SignalHandler.class);

    /**
     * Singleton instance of the client.
     */
    private static SignalHandler instance;

    /**
     * Hiding default constructor.
     */
    private SignalHandler() {

    }

    /**
     * @return the singleton instance of the signal client.
     */
    public static synchronized SignalHandler getInstance() {
        if (instance == null) {
            instance = new SignalHandler();
        }
        return instance;
    }

    /**
     * Sends the signal to the coordinator to be forwarded to the pellets.
     * @param appName application name.
     * @param pelletName pellet name to send the signal to.
     * @param data signal data.
     * @return  true if the signal was successfully sent to ZK. (This does
     * not mean signal delivery to pellets though.)
     */
    public boolean signal(final String appName, final String pelletName,
                          final byte[] data) {
        try {
            String signalPath = ZKUtils.getSingalPath(appName, pelletName);

            PelletSignal signal = new PelletSignal(appName, pelletName, data);

            byte[] ser = Utils.serialize(signal);

            if (ZKClient.getInstance().getCuratorClient().checkExists()
                    .forPath(signalPath) == null) {
                ZKClient.getInstance().getCuratorClient().create()
                        .creatingParentsIfNeeded().forPath(signalPath, ser);
            } else {
                ZKClient.getInstance().getCuratorClient().setData()
                        .forPath(signalPath, ser);
            }
        } catch (Exception e) {
            LOGGER.error("Error while connecting to Zookeeper: {}", e);
            return false;
        }
        return true;
    }


    /**
     * Sends the signal to the coordinator to be forwarded to the pellets.
     * @param appName application name.
     * @param containerName pellet name to send the signal to.
     * @param type Container signal type.
     * @param data signal data.
     * @return  true if the signal was successfully sent to ZK. (This does
     * not mean signal delivery to pellets though.)
     */
    public boolean signal(final String appName, final String containerName,
                          final ContainerSignal.ContainerSignalType type,
                          final byte[] data) {
        try {
            String signalPath = ZKUtils.getSingalPath(appName, containerName);

            ContainerSignal signal = new ContainerSignal(appName,
                    type, containerName, data);

            byte[] ser = Utils.serialize(signal);

            if (ZKClient.getInstance().getCuratorClient().checkExists()
                    .forPath(signalPath) == null) {
                ZKClient.getInstance().getCuratorClient().create()
                        .creatingParentsIfNeeded().forPath(signalPath, ser);
            } else {
                ZKClient.getInstance().getCuratorClient().setData()
                        .forPath(signalPath, ser);
            }
        } catch (Exception e) {
            LOGGER.error("Error while connecting to Zookeeper: {}", e);
            return false;
        }
        return true;
    }
}
