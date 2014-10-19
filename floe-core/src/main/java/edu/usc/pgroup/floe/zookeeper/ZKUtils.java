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

import edu.usc.pgroup.floe.coordinator.Coordinator;
import edu.usc.pgroup.floe.flake.FlakeToken;
import edu.usc.pgroup.floe.resourcemanager.ResourceMapping;
import edu.usc.pgroup.floe.thriftgen.AppStatus;
import edu.usc.pgroup.floe.utils.RetryLoop;
import edu.usc.pgroup.floe.utils.RetryPolicyFactory;
import edu.usc.pgroup.floe.utils.Utils;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;


/**
 * Floe specific utility functions for zookeeper.
 *
 * @author kumbhare
 */
public final class ZKUtils {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ZKUtils.class);

    /**
     * Hiding the default constructor.
     */
    private ZKUtils() {

    }

    /**
     * @param containerId Container Id (i.e. the node name for the container)
     * @return the full path for the container node.
     */
    public static String getContainerPath(final String containerId) {
        return ZKPaths.makePath(
                ZKConstants.Container.ROOT_NODE,
                containerId
        );
    }

    /**
     * @return the root folder path for application related data.
     */
    public static String getApplicationRootPath() {
        return ZKPaths.makePath(ZKConstants.Coordinator.ROOT_NODE,
                ZKConstants.Coordinator.APP_NODE);
    }

    /**
     * @return the root folder path for application related data.
     */
    public static String getTerminatedApplicationRootPath() {
        return ZKPaths.makePath(ZKConstants.Coordinator.ROOT_NODE,
                ZKConstants.Coordinator.TERMINATED_APP_NODE);
    }

    /**
     * @param appName the application name.
     * @return the full path for the application's root node.
     */
    public static String getApplicationPath(final String appName) {
        return ZKPaths.makePath(
            getApplicationRootPath(),
            appName
        );
    }

    /**
     * @param appName the application name.
     * @return the full path for the application's root node.
     */
    public static String getTerminatedApplicationPath(final String appName) {
        return ZKPaths.makePath(
                getTerminatedApplicationRootPath(),
                appName
        );
    }

    /**
     * @param appName the application name.
     * @return the full path for the application's satus node.
     */
    public static String getApplicationStatusPath(final String appName) {
        return ZKPaths.makePath(
                getApplicationPath(appName),
                ZKConstants.Coordinator.APP_STATUS
        );
    }

    /**
     * @param appName the application name.
     * @return the full path for the application's resource map node.
     */
    public static String getApplicationResourceMapPath(final String appName) {
        return ZKPaths.makePath(
                getApplicationPath(appName),
                ZKConstants.Coordinator.RESOURCE_MAP
        );
    }

    /**
     * @param appName the application name.
     * @return the full path where all the terminated apps are stored.
     */
    public static String getApplicationTerminatedInfoPath(
            final String appName) {
        return ZKPaths.makePath(
                getTerminatedApplicationPath(appName),
                ZKConstants.Coordinator.RESOURCE_MAP
        );
    }

    /**
     * @param appName the application name.
     * @return the full path for the application's barrier.
     */
    public static String getApplicationBarrierPath(final String appName) {
        return ZKPaths.makePath(
                getApplicationPath(appName),
                ZKConstants.Coordinator.APP_BARRIER
        );
    }

    /**
     * @param appName the application name.
     * @return Returns the application's data path. This is used to store
     * application specific data.
     */
    public static String getApplicationDataPath(final String appName) {
        return ZKPaths.makePath(
                getApplicationPath(appName),
                ZKConstants.Coordinator.APP_DATA
        );
    }

    /**
     * @param appName the application name.
     * @param pelletName pellet's name to which this flake belongs.
     * @return Returns the path for the flake, pellet and app combination.
     */
    public static String getApplicationPelletTokenPath(final String appName,
                                                      final String pelletName) {
        String tokens = ZKPaths.makePath(
                getApplicationDataPath(appName),
                ZKConstants.Coordinator.APP_FLAKE_TOKENS
        );

        String pelletTokens = ZKPaths.makePath(tokens, pelletName);

        return pelletTokens;
    }

    /**
     * @param appName the application name.
     * @param pelletName pellet's name to which this flake belongs.
     * @param fid flake id.
     * @return Returns the path for the flake, pellet and app combination.
     */
    public static String getApplicationFlakeTokenPath(final String appName,
                                                      final String pelletName,
                                                      final String fid) {
        String tokens = ZKPaths.makePath(
                getApplicationPelletTokenPath(appName, pelletName),
                fid
        );

        return tokens;
    }

    /**
     * @return the root path for coordinating signals.
     */
    public static String getSignalsRootPath() {
        return ZKPaths.makePath(ZKConstants.Coordinator.ROOT_NODE,
                ZKConstants.Coordinator.SIGNAL_NODE);
    }

    /**
     * @param appName the application name.
     * @param entityName pellet name.
     * @return the path to the signal corresponding to the give
     * application+pellet.
     */
    public static String getSingalPath(final String appName,
                                       final String entityName) {
        return ZKPaths.makePath(getSignalsRootPath(),
                appName + "-" + entityName);
    }

    /**
     * @return the path for storing cluster's current status.
     */
    public static String getClusterStatusPath() {
        return ZKPaths.makePath(ZKConstants.Coordinator.ROOT_NODE,
                ZKConstants.Coordinator.CLUSTER_STATUS);
    }


    /**
     * @param appName the name of the app to check.
     * @return true if the appName already exists, false otherwise.
     * @throws Exception if there is an exception while accessing ZK.
     */
    public static boolean appExists(final String appName) throws
            Exception {
        final String appRootPath = ZKUtils.getApplicationRootPath();
        List<String> applications;
        try {
            applications = RetryLoop.callWithRetry(
                    RetryPolicyFactory.getDefaultPolicy(),
                    new Callable<List<String>>() {
                        @Override
                        public List<String> call() throws Exception {
                            return ZKClient.getInstance()
                                    .getCuratorClient().getChildren()
                                    .forPath(appRootPath);
                        }
                    }
            );

            LOGGER.info("Running applications:{}", applications);
        } catch (Exception e) {
            LOGGER.error("Exception occurred while getting running "
                    + "applications. {}", e);
            throw new Exception(e);
        }

        if (applications.contains(appName)) {
            return true;
        }

        return false;
    }

    /**
     * Sets the cluster status.
     * @param status the new status to set.
     * @throws Exception Throws an exception if there is an error connecting
     * or setting value in the ZK.
     */
    public static void setClusterStatus(final Coordinator.ClusterStatus status)
            throws Exception {
        ZKClient.getInstance().getCuratorClient().setData()
                .forPath(ZKUtils.getClusterStatusPath(),
                        Utils.serialize(status));
    }

    /**
     * Sets the application's status.
     * @param appName App's name for which the status has to be changed.
     * @param status the new status to set.
     * @throws Exception Throws an exception if there is an error connecting
     * or setting value in the ZK.
     */
    public static void setAppStatus(final String appName,
                                    final AppStatus status)
    throws Exception {
        ZKClient.getInstance().getCuratorClient().setData()
                .forPath(ZKUtils.getApplicationStatusPath(appName),
                        Utils.serialize(status));
    }

    /**
     * gets the application's status.
     * @param appName App's name for which the status has to be changed.
     * @return the current application's status.
     * @throws Exception Throws an exception if there is an error connecting
     * or getting value from the ZK.
     */
    public static AppStatus getAppStatus(final String appName)
            throws Exception {
        byte[] bstatus = null;

        if (ZKClient.getInstance().getCuratorClient().checkExists()
                .forPath(ZKUtils.getApplicationStatusPath(appName)) == null) {
            return null;
        }

        try {
            bstatus = ZKClient.getInstance().getCuratorClient().getData()
                    .forPath(ZKUtils.getApplicationStatusPath(appName));
        } catch (Exception ex) {
            LOGGER.warn("Application does not exists, or terminated already.");
        }
        return (AppStatus) Utils.deserialize(bstatus);
    }

    /**
     * Puts or updates the resource mapping for the given application.
     * @param appName the application name.
     * @param mapping the resource mapping obtained from the scheduler?
     * @throws Exception Throws an exception if there is an error connecting
     * or setting value in the ZK.
     */
    public static void updateResourceMapping(
            final String appName,
            final ResourceMapping mapping) throws Exception {

        String rmPath = ZKUtils.getApplicationResourceMapPath(appName);

        if (ZKClient.getInstance().getCuratorClient().checkExists()
                .forPath(rmPath) == null) {
            ZKClient.getInstance().getCuratorClient().create()
                    .creatingParentsIfNeeded()
                    .forPath(rmPath,
                            Utils.serialize(mapping));
        } else {
            ZKClient.getInstance().getCuratorClient().setData()
                    .forPath(rmPath,
                            Utils.serialize(mapping));
        }
    }


    /**
     * @param appName the application name.
     * @return Gets the resource mapping for the given app.
     */
    public static ResourceMapping getResourceMapping(final String appName) {
        String resourceMappingPath = ZKUtils
                .getApplicationResourceMapPath(appName);

        byte[] serializedRM = null;

        try {
            serializedRM = ZKClient.getInstance().getCuratorClient().getData()
                    .forPath(resourceMappingPath);
        } catch (Exception e) {
            LOGGER.error("Could not receive resource mapping. Aborting.");
            return null;
        }

        ResourceMapping resourceMapping =
                (ResourceMapping) Utils.deserialize(serializedRM);

        return resourceMapping;
    }

    /**
     * Updates the token associated with the given app and flakeid.
     * Note: flake id is globally unique.
     * @param appName the application name.
     * @param pelletName pellet's name to which this flake belongs.
     * @param flakeId flake id.
     * @param token Flake's token.
     * @param flakeDataPort flake's port for data checkpointing.
     */
    public static void updateToken(final String appName,
                                   final String pelletName,
                                   final String flakeId,
                                   final Integer token,
                                   final Integer flakeDataPort) {
        String flakeTokenPath = getApplicationFlakeTokenPath(
                appName, pelletName, flakeId);

        FlakeToken ftoken = new FlakeToken(
                token,
                Utils.getHostNameOrIpAddress(),
                flakeDataPort
        );

        try {
            if (ZKClient.getInstance().getCuratorClient()
                    .checkExists().forPath(flakeTokenPath) != null) {
                ZKClient.getInstance().getCuratorClient().setData()
                    .forPath(flakeTokenPath, Utils.serialize(ftoken));
            } else {
                ZKClient.getInstance().getCuratorClient()
                        .create().creatingParentsIfNeeded()
                        .forPath(flakeTokenPath, Utils.serialize(ftoken));
            }
        } catch (Exception e) {
            LOGGER.error("Could not update flake's token.");
        }
    }

    /**
     * Updates the token associated with the given app and flakeid.
     * Note: flake id is globally unique.
     * @param appName the application name.
     * @param pelletName pellet's name to which this flake belongs.
     * @param flakeId flake id.
     */
    public static Integer getToken(final String appName,
                                   final String pelletName,
                                   final String flakeId) {
        String flakeTokenPath = getApplicationFlakeTokenPath(
                appName, pelletName, flakeId);



        try {
            if (ZKClient.getInstance().getCuratorClient()
                    .checkExists().forPath(flakeTokenPath) != null) {
                byte[] data = ZKClient.getInstance()
                        .getCuratorClient().getData()
                        .forPath(flakeTokenPath);

                FlakeToken token = (FlakeToken) Utils.deserialize(data);
                return token.getToken();
            } else {
                LOGGER.error("Fatal error. cannot continue.");
                return null;
            }
        } catch (Exception e) {
            LOGGER.error("Could not get flake's token.");
        }
        return null;
    }

    /**
     * removes the flake from the token ring in Zookeeper.
     * @param appName the application name.
     * @param pelletName pellet's name to which this flake belongs.
     * @param flakeId flake id.
     */
    public static void removeNeighbor(final String appName,
                                      final String pelletName,
                                      final String flakeId) {

        String flakeTokenPath = getApplicationFlakeTokenPath(
                appName, pelletName, flakeId);

        try {
            if (ZKClient.getInstance().getCuratorClient()
                    .checkExists().forPath(flakeTokenPath) != null) {
                LOGGER.error("DELETEING FLAKE FROM RING.");
                ZKClient.getInstance().getCuratorClient().delete()
                        .deletingChildrenIfNeeded()
                        .forPath(flakeTokenPath);
            }
        } catch (Exception e) {
            LOGGER.error("Could not update flake's token.");
        }
    }
}
