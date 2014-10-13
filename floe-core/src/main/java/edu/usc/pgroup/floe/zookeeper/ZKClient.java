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

package edu.usc.pgroup.floe.zookeeper;


import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.container.ContainerInfo;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.zkcache.PathChildrenUpdateListener;
import org.apache.commons.lang.StringUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.curator.utils.CloseableUtils;
import org.apache.curator.utils.EnsurePath;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The singleton class for all zookeeper access.
 *
 * @author Alok Kumbhareutils.Utils;
 */
public final class ZKClient {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ZKClient.class);
    /**
     * singleton ZKClient instance.
     */
    private static ZKClient instance;

    /**
     * The test ZK server used during local (pseudo-distributed) mode.
     * NOTE: NOT REQUIRED?
     */
    //private TestingServer localZKServer;
    /**
     * curator client.
     */
    private CuratorFramework curatorClient;

    /**
     * hiding the default constructor.
     */
    private ZKClient() {

    }

    /**
     * Configures and returns a singleton ZKClient object.
     * This provides a FLOE specific high level API to interact with Zookeeper.
     *
     * @return ZKClient singleton instance.
     */
    public static synchronized ZKClient getInstance() {
        if (instance == null) {
            instance = new ZKClient();
            try {
                instance.initialize();
            } catch (Exception e) {
                LOGGER.error(e.getMessage());
                throw new RuntimeException("Error occurred while connecting "
                        + "to Zookeeper server");
            }
        }
        return instance;
    }

    /**
     * Initializes the curator client and connects to the ZK server.
     * 1. Checks to see if a previous session may be used (TODO)
     * 2. If yes, reconnects to that session using the session id and password.
     * 3. otherwise, creates a new connection to ZK.
     * Further, it checks the "floe.execution.mode",
     * and launches an in-memory ZK test server for local mode.
     * TODO: Zookeeper Authorization.
     *
     * @throws java.lang.Exception (from the curator testing server) or from
     *                             Buider.build
     */
    private void initialize() throws Exception {
        String zkConnectionString = null;
        String executionMode = FloeConfig.getConfig().getString(
                ConfigProperties.FLOE_EXEC_MODE);
        LOGGER.info("Initializing zookeeper client.");
        if (executionMode.equalsIgnoreCase(Utils.Constants.LOCAL)) {

            LOGGER.info("Using Zookeeper local testing server.");
            /*localZKServer = new TestingServer();
            zkConnectionString = localZKServer.getConnectString();*/
            zkConnectionString = "localhost:2181"; //FIXME

        } else if (executionMode.equalsIgnoreCase(Utils.Constants
                .DISTRIBUTED)) {
            LOGGER.info("Connecting to the Zookeeper ensemble.");

            List<String> serverPorts = new ArrayList<String>();
            String[] zkServers = ZKConstants.SERVERS;
            for (String server : zkServers) {
                serverPorts.add(server + ":" + ZKConstants.PORT);
            }

            zkConnectionString = StringUtils.join(serverPorts, ",");
        } else {
            LOGGER.error("Invalid floe.execution.mode: '" + executionMode
                    + "' Should be either '"
                    + Utils.Constants.LOCAL + "' or '"
                    + Utils.Constants.DISTRIBUTED + "'.");
            throw new RuntimeException(new IllegalArgumentException(
                    "executionMode"));
        }

        int retryTimeout = FloeConfig.getConfig().getInt(
                ConfigProperties.ZK_RETRY_TIMEOUT);

        int retryAttempts = FloeConfig.getConfig().getInt(
                ConfigProperties.ZK_RETRY_ATTEMPTS);

        createRootIfNotExists(zkConnectionString, ZKConstants.FLOE_ROOT,
                retryTimeout, retryAttempts
        );

        zkConnectionString += ZKConstants.FLOE_ROOT;

        //TODO: Fetch previous session.
        /*if(Utils.prevSessionExists()) {

        } else {

        }*/
        LOGGER.info("Zookeeper connection string: " + zkConnectionString);

        CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory
                .builder()
                .connectString(zkConnectionString)
                .retryPolicy(
                        new ExponentialBackoffRetry(retryTimeout, retryAttempts)
                );
        //TODO: take backoff time from config.
        //TODO: .connectionTimeoutMs()
        //TODO: .sessionTimeoutMs()

        //TODO: Zookeeper authorization here
        curatorClient = builder.build();
        curatorClient.start();
    }

    /**
     * Function to create the root ZK dir if it does not exist.
     *
     * @param zkConnectionString ZK connection string, without the root
     * @param floeRoot           Floe root dir
     * @param retryTimeout       connection timeout in ms
     * @param retryAttempts      retry attempts (for exponential backoff)
     */
    private void createRootIfNotExists(final String zkConnectionString,
                                       final String floeRoot,
                                       final int retryTimeout,
                                       final int retryAttempts) {
        CuratorFramework curatorClientForChRoot = null;
        try {
            CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory
                    .builder()
                    .connectString(zkConnectionString)
                    .retryPolicy(
                            new ExponentialBackoffRetry(retryTimeout,
                                    retryAttempts)
                    );
            //TODO: take backoff time from config.
            //TODO: .connectionTimeoutMs()
            //TODO: .sessionTimeoutMs()

            //TODO: Zookeeper authorization here
            curatorClientForChRoot = builder.build();
            curatorClientForChRoot.start();

            EnsurePath ensurePath = new EnsurePath(floeRoot);
            ensurePath.ensure(curatorClientForChRoot.getZookeeperClient());
            LOGGER.info("Root folder exists or created.");
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Could not create root folder.");
            throw new RuntimeException(e);
        } finally {
            CloseableUtils.closeQuietly(curatorClientForChRoot);
        }
    }

    /**
     * Updates the ZK container node with the container info.
     *
     * @param cinfo current ContainerInfo object.
     */
    public void sendContainerHeartBeat(final ContainerInfo cinfo) {
        String containerPath = ZKUtils.getContainerPath(cinfo.getContainerId());
        LOGGER.debug("Container Path:" + containerPath);
        EnsurePath ensurePath = new EnsurePath(containerPath);
        try {
            ensurePath.ensure(curatorClient.getZookeeperClient());

            byte[] searializedCinfo = Utils.serialize(cinfo);

            curatorClient.setData().forPath(containerPath, searializedCinfo);
            LOGGER.debug("Updated container info for: "
                    + containerPath);
        } catch (Exception e) {
            LOGGER.error("Could not create ZK node or set heartbeat for the "
                    + "container.");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }


    /**
     * Creates a cache for the child nodes, and subscribes for any updates.
     * (add, update, delete).
     *
     * @param path               Path of the parent node.
     * @param pathUpdateListener Floe's Path update listener.
     * @param cacheData          If true, each child node's data is cached
     *                           along with the stat information.
     * @return Curator client's cache object. TODO: Change this to higher
     * level abstraction.
     */
    public PathChildrenCache cacheAndSubscribeChildren(
            final String path,
            final PathChildrenUpdateListener pathUpdateListener,
            final boolean cacheData) {

        PathChildrenCache cache = new PathChildrenCache(
                curatorClient, path, cacheData
        );

        if (pathUpdateListener != null) {
            PathChildrenCacheListener cacheListener =
                new PathChildrenCacheListener() {
                    @Override
                    public void childEvent(
                            final CuratorFramework curatorFramework,
                            final PathChildrenCacheEvent pathChildrenCacheEvent)
                            throws Exception {
                        switch (pathChildrenCacheEvent.getType()) {
                            case CHILD_ADDED:
                                pathUpdateListener.childAdded(
                                        pathChildrenCacheEvent.getData()
                                );
                                break;
                            case CHILD_UPDATED:
                                pathUpdateListener.childUpdated(
                                        pathChildrenCacheEvent.getData()
                                );
                                break;
                            case CHILD_REMOVED:
                                pathUpdateListener.childRemoved(
                                        pathChildrenCacheEvent.getData()
                                );
                                break;
                            case INITIALIZED:
                                pathUpdateListener.childrenListInitialized(
                                        pathChildrenCacheEvent.getInitialData()
                                );
                                break;
                            default:
                            /*
                            Ignore other pathChildrenEvents. These are handled
                            internally by the curator's zkcache framework.
                             */
                        }
                    }
                };
            cache.getListenable().addListener(cacheListener);
        }

        try {
            cache.start(PathChildrenCache.StartMode.BUILD_INITIAL_CACHE);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Could not start cache client.");
            throw new RuntimeException(e);
        }
        return cache;
    }

    /**
     * Returns the curator client. THis is used only for testing.
     * Todo: Delete this.
     *
     * @return initialized curator client instance.
     */
    public CuratorFramework getCuratorClient() {
        return curatorClient;
    }
}
