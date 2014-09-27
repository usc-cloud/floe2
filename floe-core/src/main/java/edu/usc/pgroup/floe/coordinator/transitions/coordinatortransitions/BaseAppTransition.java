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

package edu.usc.pgroup.floe.coordinator.transitions.coordinatortransitions;

/**
 * @author kumbhare
 */

import edu.usc.pgroup.floe.coordinator.transitions.ClusterTransition;
import edu.usc.pgroup.floe.resourcemanager.ResourceMapping;
import edu.usc.pgroup.floe.signals.ContainerSignal;
import edu.usc.pgroup.floe.signals.SignalHandler;
import edu.usc.pgroup.floe.thriftgen.AppStatus;
import edu.usc.pgroup.floe.thriftgen.InsufficientResourcesException;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Transition to start a new application.
 */
public abstract class BaseAppTransition extends ClusterTransition {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(BaseAppTransition.class);


    /**
     * Start the execution for the given transition. Returns immediately
     * after starting?
     *
     * @param args transaction specific arguments
     * @throws Exception if there is an unrecoverable error while
     *                   processing the transition.
     */
    @Override
    protected final void execute(final Map<String, Object> args)
            throws Exception {
        LOGGER.info("Executing StartApp transition.");

        String appName = (String) args.get("appName");

        TFloeApp app = (TFloeApp) args.get("app");

        ResourceMapping currentMapping
                = ZKUtils.getResourceMapping(appName);

        if (!preTransition(appName, currentMapping)) {
            LOGGER.error("Pre-Transition failed.");
            throw new Exception("Transition cannot be executed. "
                    + "Pre-transition failed.");
        }


        //STEP 1: Schedule app, Get resource mapping and update it in ZK
        ResourceMapping updatedMapping = schedule(appName, app,
                currentMapping, args);

        String appUpdateBarrierPath = ZKUtils
                .getApplicationBarrierPath(appName);

        int numContainersToUpdate = updatedMapping.getContainersToUpdate();

        DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(
                ZKClient.getInstance().getCuratorClient(),
                appUpdateBarrierPath,
                numContainersToUpdate + 1
        );

        //Step 2. Send the command to *ALL* containers to check the
        // resource mapping and CREATE the required flakes.
        createFlakes(updatedMapping, barrier);
        LOGGER.info("All containers finished launching flakes.");


        //Step 3. Send connect signals to flakes so that they establish all
        // the appropriate channels.
        connectFlakes(updatedMapping, barrier);
        LOGGER.info("All containers finished CONNECTING flakes.");

        //Step 4. wait for all containers to finish launching or removing
        // pellets.
        increaseOrDecreasePellets(updatedMapping, barrier);
        LOGGER.info("All pellets successfully created.");

        //Step 5. wait for container to terminate flakes, if required.
        terminateFlakes(updatedMapping, barrier);
        LOGGER.info("All required flakes terminated.");

        //Step 6. Send signal Start pellets.
        startPellets(updatedMapping, barrier);
        LOGGER.info("All pellets Started. The application is now "
                + "running");
    }

    /**
     * Send TerminateFlakes requests to all containers and waits for their
     * response.
     * @param updatedMapping the resource mapping to use.
     * @param barrier The barrier used for synchronization.
     * @throws Exception if an error occurs.
     */
    private void terminateFlakes(final ResourceMapping updatedMapping,
                              final DistributedDoubleBarrier barrier)
            throws Exception {

        String appName = updatedMapping.getAppName();

        SignalHandler.getInstance().signal(appName, "ALL-CONTAINERS",
                ContainerSignal.ContainerSignalType.TERMINATE_FLAKES,
                Utils.serialize("dummy"));

        ZKUtils.setAppStatus(appName,
                AppStatus.UPDATING_FLAKES);

        barrier.enter(); //wait for all containers to receive the new
        // resource mapping and start processing.

        LOGGER.info("Waiting for containers to finish flake deployment.");

        barrier.leave(); //wait for all containers to deploy (or update)
        // flakes and complete their execution.

        ZKUtils.setAppStatus(appName, AppStatus.UPDATING_FLAKES_COMPLETED);
    }

    /**
     * Send start pellets requests to all containers and waits for
     * their response.
     * @param updatedMapping the resource mapping to use.
     * @param barrier The barrier used for synchronization.
     * @throws Exception if an error occurs.
     */
    private void startPellets(final ResourceMapping updatedMapping,
                              final DistributedDoubleBarrier barrier)
            throws Exception {

        String appName = updatedMapping.getAppName();
        SignalHandler.getInstance().signal(appName, "ALL-CONTAINERS",
                ContainerSignal.ContainerSignalType.START_PELLETS,
                Utils.serialize("dummy"));

        ZKUtils.setAppStatus(appName,
                AppStatus.STARTING_PELLETS);

        barrier.enter();
        LOGGER.info("Waiting for containers to Start all pellets.");
        barrier.leave();
        ZKUtils.setAppStatus(appName,
                AppStatus.RUNNING);
    }


    /**
     * Send Connect or disconnect requests to all containers and waits for
     * their response.
     * @param updatedMapping the resource mapping to use.
     * @param barrier The barrier used for synchronization.
     * @throws Exception if an error occurs.
     */
    private void connectFlakes(final ResourceMapping updatedMapping,
                               final DistributedDoubleBarrier barrier)
            throws Exception {

        String appName = updatedMapping.getAppName();

        SignalHandler.getInstance().signal(appName, "ALL-CONTAINERS",
            ContainerSignal.ContainerSignalType.CONNECT_OR_DISCONNECT_FLAKES,
            Utils.serialize("dummy"));

        barrier.enter();

        LOGGER.info("Waiting for containers to finish flake channel creation.");

        barrier.leave();

        ZKUtils.setAppStatus(appName,
                AppStatus.UPDATING_FLAKES_COMPLETED);
    }

    /**
     * Send Increase or decrease pellets requests to all containers and waits
     * for their response.
     * @param updatedMapping the resource mapping to use.
     * @param barrier The barrier used for synchronization.
     * @throws Exception if an error occurs.
     */
    private void increaseOrDecreasePellets(
            final ResourceMapping updatedMapping,
            final DistributedDoubleBarrier barrier)
            throws Exception {

        String appName = updatedMapping.getAppName();
        SignalHandler.getInstance().signal(appName, "ALL-CONTAINERS",
            ContainerSignal.ContainerSignalType.INCREASE_OR_DECREASE_PELLETS,
            Utils.serialize("dummy"));

        ZKUtils.setAppStatus(appName,
                AppStatus.UPDATING_PELLETS);

        barrier.enter();
        LOGGER.info("Waiting for containers to launch pellets in the flakes");
        barrier.leave();
        ZKUtils.setAppStatus(appName,
                AppStatus.UPDATING_PELLETS_COMPLETED);
    }

    /**
     * Send CreateFlake requests to all containers and waits for their response.
     * @param updatedMapping the resource mapping to use.
     * @param barrier The barrier used for synchronization.
     * @throws Exception if an error occurs.
     */
    private void createFlakes(final ResourceMapping updatedMapping,
                              final DistributedDoubleBarrier barrier)
            throws Exception {

        String appName = updatedMapping.getAppName();

        SignalHandler.getInstance().signal(appName, "ALL-CONTAINERS",
                ContainerSignal.ContainerSignalType.CREATE_FLAKES,
                Utils.serialize("dummy"));

        ZKUtils.setAppStatus(appName,
                AppStatus.UPDATING_FLAKES);

        barrier.enter(); //wait for all containers to receive the new
        // resource mapping and start processing.

        LOGGER.info("Waiting for containers to finish flake deployment.");

        barrier.leave(); //wait for all containers to deploy (or update)
        // flakes and complete their execution.

        ZKUtils.setAppStatus(appName, AppStatus.UPDATING_FLAKES_COMPLETED);
    }

    /**
     * Updates the resource mapping for the application.
     * @param appName the applicaiton name.
     * @param app The TFloeApp applicaiton object.
     * @param currentMapping current resource mapping.
     * @param args transaction specific arguments
     * @return updated resource mapping.
     * @throws Exception if an error occurs.
     */
    private ResourceMapping schedule(final String appName,
                                     final TFloeApp app,
                                     final ResourceMapping currentMapping,
                                     final Map<String, Object> args)
            throws Exception {

        ZKUtils.setAppStatus(appName, AppStatus.SCHEDULING);

        ResourceMapping updatedMapping = updateResourceMapping(
                appName, app, currentMapping, args);

        LOGGER.info("Planned initial resource mapping:" + updatedMapping);

        if (updatedMapping == null) {
            LOGGER.warn("Insufficient resources to deploy the application.");
            throw new InsufficientResourcesException("Unable to acquire "
                    + "required resources.");
        }

        //STEP 3: Put the resource Mapping in ZK.
        ZKUtils.updateResourceMapping(appName, updatedMapping);

        ZKUtils.setAppStatus(appName,
                AppStatus.SCHEDULING_COMPLETED);

        return updatedMapping;
    }

    /**
     * Pre-transition activities (like verification of topology,
     * app exists etc.).
     * @param appName the applicaiton name.
     * @param mapping Current Resource Mapping. (null for a new deployment)
     * @return True if preTransition was successful and the transition itself
     * should be executed. False implies that there are an error and the
     * transition should be skipped.
     */
    public abstract boolean preTransition(
            final String appName,
            final ResourceMapping mapping);

    /**
     * Post-transition activities (like move app to archive etc.).
     * @param mapping Updated Resource Mapping.
     * @return True if postTransition was successful, false otherwise.
     */
    public abstract boolean postTransition(final ResourceMapping mapping);

    /**
     * Transition specific update resource mapping function.
     * @param appName the applicaiton name.
     * @param app The TFloeApp applicaiton object.
     * @param currentMapping Current Resource mapping.
     * @param args transaction specific arguments
     * @return updated resource mapping based on the transition.
     */
    public abstract ResourceMapping updateResourceMapping(
            final String appName,
            final TFloeApp app,
            final ResourceMapping currentMapping,
            final Map<String, Object> args);
}
