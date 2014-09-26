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

package edu.usc.pgroup.floe.coordinator;

import edu.usc.pgroup.floe.coordinator.transitions.Transitions;
import edu.usc.pgroup.floe.coordinator
        .transitions.coordinatortransitions.KillAppTransition;
import edu.usc.pgroup.floe.coordinator
        .transitions.coordinatortransitions.StartAppTransition;
import edu.usc.pgroup.floe.resourcemanager.ResourceManager;
import edu.usc.pgroup.floe.resourcemanager.ResourceManagerFactory;
import edu.usc.pgroup.floe.thriftgen.AppStatus;
import edu.usc.pgroup.floe.thriftgen.ScaleDirection;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * The main class for all coordination (start, stop, floes,
 * assign resources etc.) and monitoring activities.
 *
 * @author kumbhare
 */
public final class Coordinator {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(Coordinator.class);

    /**
     * Resource manager.
     */
    private ResourceManager resourceManager;

    /**
     * singleton class instance.
     */
    private static Coordinator instance;

    /**
     * hiding the default constructor.
     */
    private Coordinator() {

    }

    /**
     * returns the singleton instance. This is thread safe.
     *
     * @return single coordinator instance.
     */
    public static synchronized Coordinator getInstance() {
        if (instance == null) {
            instance = new Coordinator();
            instance.initialize();
        }
        return instance;
    }

    /**
     * Initializer. Sets up various zookeeper watches etc.
     */
    private void initialize() {
        LOGGER.info("Initializing resource manager.");
        resourceManager = ResourceManagerFactory.getResourceManager();
        try {
            //Create Coordinator folder, and Apps root folder.
            //NOT REQUIRED. THE CACHE
            if (ZKClient.getInstance().getCuratorClient().checkExists()
                    .forPath(ZKUtils.getApplicationRootPath()) == null) {
                ZKClient.getInstance().getCuratorClient().create()
                        .creatingParentsIfNeeded()
                        .forPath(ZKUtils.getApplicationRootPath());
            }


            if (ZKClient.getInstance().getCuratorClient().checkExists()
                    .forPath(ZKUtils.getClusterStatusPath()) == null) {
                ZKClient.getInstance().getCuratorClient().create()
                        .creatingParentsIfNeeded()
                        .forPath(ZKUtils.getClusterStatusPath(),
                                Utils.serialize(
                                        Coordinator.ClusterStatus.AVAILABLE));
            }

            LOGGER.info("Coordinator root and apps folder created: {}.",
                    ZKUtils.getApplicationRootPath());
        } catch (KeeperException.NodeExistsException e) {
            LOGGER.warn("Already exits. Not creating again.:{}",
                    ZKUtils.getApplicationRootPath());
        } catch (Exception e) {
            LOGGER.error("Error occurred while creating: {}", e);
        }
    }

    /**
     * @param appName application name.
     * @return the current status of the application.
     * @throws Exception if the given application is not running.
     */
    public AppStatus getApplicationStatus(final String appName)
            throws Exception {
        return ZKUtils.getAppStatus(appName);
    }

    /**
     * Enum for cluster transactions.
     */
    public enum ClusterTransitions {
        /**
         * Cluster transition to start a new application.
         */
        START_APP,
        /**
         * Cluster transition to stop an application.
         */
        STOP_APP
    }

    /**
     * Enum for App transitions.
     */
    public enum AppTransitions {
        /**
         * Application transition to start scale up a given application.
         */
        SCALE_UP,
        /**
         * Application transition to start scale down a given application.
         */
        SCALE_DOWN,
        /**
         * Application transition to switch alternates for a given application.
         */
        SWITCH_ALTERNATE
    }


    /**
     * Enum for cluster status.
     */
    public enum ClusterStatus {
        /**
         * Status to indicate that coordinator is currently busy.
         */
        BUSY,
        /**
         * Status to indicate that the coordinator is available and can take
         * more requests.
         */
        AVAILABLE
    }

    /**
     * Acquire required resources and submit a floe app.
     * @param appName name of the app
     * @param app The app topology.
     * @throws org.apache.thrift.TException thrift exception wrapper.
     */
    public void submitApp(final String appName, final TFloeApp app) throws
            TException {
        Map<String, Object> args = new HashMap<>();
        args.put("appName", appName);
        args.put("app", app);
        try {
            Transitions.execute(new StartAppTransition(),
                                args);
        } catch (Exception e) {
            LOGGER.error("Could not start app: {}", e);
            throw new TException(e);
        }
    }

    /**
     * Kills the application.
     * @param appName application name.
     * @throws org.apache.thrift.TException thrift exception wrapper.
     */
    public void killApp(final String appName) throws TException {
        Map<String, Object> args = new HashMap<>();
        args.put("appName", appName);
        try {
        Transitions.execute(new KillAppTransition(),
                            args);
        } catch (Exception e) {
            LOGGER.error("Could not kill app: {}", e);
            throw new TException(e);
        }
    }

    /**
     * Service call to handle the scale event at runtime.
     * @param direction direction of scaling
     * @param appName name of the app
     * @param pelletName name of the pellet
     * @param count number of instances to be scaled up or down.
     * throws InsufficientResourcesException if enough containers are not
     * available.
     * throws AppNotFoundException if the given appName does is not running.
     * throws PelletNotFoundException if the application does not contain a
     * pellet with the given name.
     * @throws TException Any exceptions wrapped into TException.
     */
    public void scale(final ScaleDirection direction,
                      final String appName,
                      final String pelletName,
                      final int count) throws TException {
        LOGGER.info("Received scale app request for: {}", appName);
    }

    /**
     * Service call to handle switch alternate.
     * @param appName name of the app.
     * @param pelletName name of the pellet.
     * @param alternateName alternate to switch to.
     * @throws TException TException Thrift exception wrapper.
     */
    public void switchAlternate(final String appName,
                                final String pelletName,
                                final String alternateName)
            throws TException {
        LOGGER.info("Received switch alternate request for: {}", appName);
    }
}
