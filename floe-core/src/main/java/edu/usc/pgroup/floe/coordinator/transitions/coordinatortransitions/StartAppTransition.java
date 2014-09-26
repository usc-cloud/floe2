package edu.usc.pgroup.floe.coordinator.transitions.coordinatortransitions;

/**
 * @author kumbhare
 */

import edu.usc.pgroup.floe.coordinator.transitions.ClusterTransition;
import edu.usc.pgroup.floe.resourcemanager.ResourceManagerFactory;
import edu.usc.pgroup.floe.resourcemanager.ResourceMapping;
import edu.usc.pgroup.floe.signals.ContainerSignal;
import edu.usc.pgroup.floe.signals.SignalHandler;
import edu.usc.pgroup.floe.thriftgen.AppStatus;
import edu.usc.pgroup.floe.thriftgen.DuplicateException;
import edu.usc.pgroup.floe.thriftgen.InsufficientResourcesException;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Transition to start a new application.
 */
public class StartAppTransition extends ClusterTransition {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(StartAppTransition.class);


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

        //STEP 1. Verify application.

        //STEP 1a. Verify if the name exists.
        try {
            if (ZKUtils.appExists(appName)) {
                LOGGER.error("Application name already exists.");
                throw new DuplicateException();
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred while checking existing "
                    + "applications: {}", e);
            throw new TException(e);
        }

        //STEP 1b. Reserve the app name and set status to
        // "Request received".
        String appPath = ZKUtils.getApplicationPath(appName);

        String appStatusPath = ZKUtils.getApplicationStatusPath(appName);

        LOGGER.info("App Path to store the configuration:" + appPath);
        ZKClient.getInstance().getCuratorClient()
                .create().creatingParentsIfNeeded()
                .forPath(appStatusPath,
                        Utils.serialize(
                            AppStatus.NEW_DEPLOYMENT_REQ_RECEIVED));


        //STEP 1c. Verify topology. (TODO)

        //STEP 2: Get resource mapping and update it in ZK
        ZKUtils.setAppStatus(appName, AppStatus.SCHEDULING);

        ResourceMapping mapping = ResourceManagerFactory.getResourceManager()
                .getInitialMapping(appName, app);
        LOGGER.info("Planned initial resource mapping:" + mapping);

        if (mapping == null) {
            LOGGER.warn("Insufficient resources to deploy the application.");
            throw new InsufficientResourcesException("Unable to acquire "
                    + "required resources.");
        }

        //STEP 3: Put the resource Mapping in ZK and wait for each container
        // to pull the data and start the flakes.
        ZKUtils.updateResourceMapping(appName, mapping);

        ZKUtils.setAppStatus(appName,
                AppStatus.SCHEDULING_COMPLETED);

        String appUpdateBarrierPath = ZKUtils
                .getApplicationBarrierPath(appName);

        int numContainersToUpdate = mapping.getContainersToUpdate();

        DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(
                ZKClient.getInstance().getCuratorClient(),
                appUpdateBarrierPath,
                numContainersToUpdate + 1
        );

        //Step 3a. Send the command to *ALL* containers to check the
        // resource mapping and deploy the required flakes.

        LOGGER.info("Entering barrier: {} for {} count",
                appUpdateBarrierPath, numContainersToUpdate + 1);

        SignalHandler.getInstance().signal(appName, "ALL-CONTAINERS",
                ContainerSignal.ContainerSignalType.CREATE_FLAKES,
                Utils.serialize("dummy"));

        ZKUtils.setAppStatus(appName,
                AppStatus.UPDATING_FLAKES);

        barrier.enter(); //wait for all containers to receive the new
        // resource mapping and start processing.

        LOGGER.info("Waiting for containers to finish flake deployment.");

        //Step 3b. wait for all containers to finish starting flakes.
        barrier.leave(); //wait for all containers to deploy (or update)
        // flakes and complete their execution.

        LOGGER.info("All containers finished launching flakes.");

        //Step 3c. Send connect signals to flakes so that they establish all
        // the appropriate channels.
        SignalHandler.getInstance().signal(appName, "ALL-CONTAINERS",
                ContainerSignal.ContainerSignalType.CONNECT_FLAKES,
                Utils.serialize("dummy"));

        barrier.enter();

        LOGGER.info("Waiting for containers to finish flake channel creation.");

        barrier.leave();

        LOGGER.info("All containers finished connecting flakes.");

        ZKUtils.setAppStatus(appName,
                AppStatus.UPDATING_FLAKES_COMPLETED);


        //Step 5. Send signal to Launch Pellets
        SignalHandler.getInstance().signal(appName, "ALL-CONTAINERS",
                ContainerSignal.ContainerSignalType.LAUNCH_PELLETS,
                Utils.serialize("dummy"));

        ZKUtils.setAppStatus(appName,
                AppStatus.UPDATING_PELLETS);

        barrier.enter();
        LOGGER.info("Waiting for containers to launch pellets in the flakes");
        barrier.leave();
        ZKUtils.setAppStatus(appName,
                AppStatus.UPDATING_PELLETS_COMPLETED);


        LOGGER.info("All pellets successfully created.");

        //Step 6. Send signal Start pellets.
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

        LOGGER.info("All pellets Started. The application is now "
                + "running");
    }

    /**
     * @return gets the name of the transaction.
     */
    @Override
    public final String getName() {
        return "StartAppTransition";
    }
}
