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

import edu.usc.pgroup.floe.resourcemanager.ResourceManager;
import edu.usc.pgroup.floe.resourcemanager.ResourceManagerFactory;
import edu.usc.pgroup.floe.resourcemanager.ResourceMapping;
import edu.usc.pgroup.floe.thriftgen.AppNotFoundException;
import edu.usc.pgroup.floe.thriftgen.DuplicateException;
import edu.usc.pgroup.floe.thriftgen.InsufficientResourcesException;
import edu.usc.pgroup.floe.thriftgen.ScaleDirection;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.utils.RetryLoop;
import edu.usc.pgroup.floe.utils.RetryPolicyFactory;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.apache.thrift.TException;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * The main class for all coordination (start, stop, floes,
 * assign resources etc.) and monitoring activities.
 * TODO: Refactor class name.
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
     * Acquire required resources and submit a floe app.
     * @param appName name of the app
     * @param app The app topology.
     * @throws org.apache.thrift.TException thrift exception wrapper.
     */
    public void submitApp(final String appName, final TFloeApp app) throws
            TException {

        LOGGER.info("Received submit app request for: {}", appName);

        //verify name does not exist.
        try {
            if (appExists(appName)) {
                LOGGER.error("Application name already exists.");
                throw new DuplicateException();
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred while checking existing "
                    + "applications: {}", e);
            throw new TException(e);
        }

        //verify topology (TODO).

        //get the resource manager and request for resource mapping from
        // scheduler.
        ResourceMapping mapping = ResourceManagerFactory.getResourceManager()
                .getInitialMapping(appName, app);
        LOGGER.info("Planned initial resource mapping:" + mapping);

        if (mapping == null) {
            LOGGER.warn("Insufficient resources to deploy the application.");
            throw new InsufficientResourcesException("Unable to acquire "
                    + "required resources.");
        }


        //Put the mapping into ZK for each container to start pulling data.
        String appPath = ZKUtils.getApplicationPath(appName);
        LOGGER.info("App Path to store the configuration:" + appPath);

        try {
            ZKClient.getInstance().getCuratorClient()
                    .create().creatingParentsIfNeeded()
                    .forPath(appPath,
                    Utils.serialize(mapping));
        } catch (Exception e) {
            LOGGER.error("Could not access ZK to store the application "
                    + "mapping");
            throw new TException(e);
        }

        //wait for topology to start.
    }

    /**
     * @param appName the name of the app to check.
     * @return true if the appName already exists, false otherwise.
     * @throws Exception if there is an exception while accessing ZK.
     */
    private boolean appExists(final String appName) throws Exception {
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

        //verify name does not exist.
        try {
            if (!appExists(appName)) {
                LOGGER.error("Application does not exist.");
                throw new AppNotFoundException();
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred while checking existing "
                    + "applications: {}", e);
            throw new TException(e);
        }

        //get current resource mapping from zk.
        String appPath = ZKUtils.getApplicationPath(appName);
        ResourceMapping currentMapping;
        LOGGER.info("App Path to get the configuration:" + appPath);
        try {
            byte[] childData = ZKClient.getInstance().getCuratorClient()
                    .getData().forPath(appPath);

            currentMapping  = (ResourceMapping) Utils.deserialize(childData);
        } catch (Exception e) {
            LOGGER.error("Could not access ZK to store the application "
                    + "mapping");
            throw new TException(e);
        }

        //get the resource manager and request for resource mapping from
        // scheduler.
        ResourceMapping mapping = ResourceManagerFactory.getResourceManager()
                .scale(currentMapping, direction, pelletName, count);

        LOGGER.info("New resource mapping: {}", mapping);
        LOGGER.info("Resource Mapping Delta: {}", mapping.getDelta());

        if (mapping == null) {
            LOGGER.warn("Insufficient resources to deploy the application.");
            throw new InsufficientResourcesException("Unable to acquire "
                    + "required resources.");
        }


        //Put the updated mapping back into ZK for each container to start
        // pulling data.
        LOGGER.info("App Path to store the configuration:" + appPath);

        try {
            ZKClient.getInstance().getCuratorClient()
                    .setData().forPath(appPath,
                            Utils.serialize(mapping));
        } catch (Exception e) {
            LOGGER.error("Could not access ZK to store the application "
                    + "mapping");
            throw new TException(e);
        }
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

        //verify name does not exist.
        try {
            if (!appExists(appName)) {
                LOGGER.error("Application does not exist.");
                throw new AppNotFoundException();
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred while checking existing "
                    + "applications: {}", e);
            throw new TException(e);
        }

        //get current resource mapping from zk.
        String appPath = ZKUtils.getApplicationPath(appName);
        ResourceMapping currentMapping;
        LOGGER.info("App Path to get the configuration:" + appPath);
        try {
            byte[] childData = ZKClient.getInstance().getCuratorClient()
                    .getData().forPath(appPath);

            currentMapping  = (ResourceMapping) Utils.deserialize(childData);
        } catch (Exception e) {
            LOGGER.error("Could not access ZK to store the application "
                    + "mapping");
            throw new TException(e);
        }

        //get the resource manager and request for resource mapping from
        // scheduler.
        /*ResourceMapping mapping = ResourceManagerFactory.getResourceManager()
                .scale(currentMapping, direction, pelletName, count);*/

        ResourceMapping mapping = ResourceManagerFactory.getResourceManager()
                .switchAlternate(currentMapping, pelletName, alternateName);

        LOGGER.info("New resource mapping: {}", mapping);
        LOGGER.info("Resource Mapping Delta: {}", mapping.getDelta());

        if (mapping == null) {
            LOGGER.warn("Insufficient resources to deploy the application.");
            throw new InsufficientResourcesException("Unable to acquire "
                    + "required resources.");
        }


        //Put the updated mapping back into ZK for each container to start
        // pulling data.
        LOGGER.info("App Path to store the configuration:" + appPath);

        try {
            ZKClient.getInstance().getCuratorClient()
                    .setData().forPath(appPath,
                    Utils.serialize(mapping));
        } catch (Exception e) {
            LOGGER.error("Could not access ZK to store the application "
                    + "mapping");
            throw new TException(e);
        }
    }
}
