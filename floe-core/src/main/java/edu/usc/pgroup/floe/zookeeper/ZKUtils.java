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
        byte[] bstatus = ZKClient.getInstance().getCuratorClient().getData()
                .forPath(ZKUtils.getApplicationStatusPath(appName));
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


}
