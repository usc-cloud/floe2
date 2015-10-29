package edu.usc.pgroup.floe.coordinator.transitions.coordinatortransitions;

/**
 * @author kumbhare
 */

import edu.usc.pgroup.floe.resourcemanager.ResourceManagerFactory;
import edu.usc.pgroup.floe.resourcemanager.ResourceMapping;
import edu.usc.pgroup.floe.thriftgen.AppStatus;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * Transition to start a new application.
 */
public class StartAppTransition extends BaseAppTransition {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(StartAppTransition.class);

    /**
     * Pre-transition activities (like verification of topology,
     * app exists etc.).
     * @param appName the applicaiton name.
     * @param mapping Current Resource Mapping. (null for a new deployment)
     * @return True if preTransition was successful and the transition itself
     * should be executed. False implies that there are an error and the
     * transition should be skipped.
     */
    @Override
    public final boolean preTransition(
            final String appName,
            final ResourceMapping mapping) {

        //STEP 1. Verify application.

        //STEP 1a. Verify if the name exists.
        try {
            if (ZKUtils.appExists(appName)) {
                LOGGER.error("Application name already exists.");
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Error occurred while checking existing "
                    + "applications: {}", e);
            return false;
        }

        //STEP 1b. Reserve the app name and set status to
        // "Request received".
        String appPath = ZKUtils.getApplicationPath(appName);

        String appStatusPath = ZKUtils.getApplicationStatusPath(appName);

        LOGGER.info("App Path to store the configuration:" + appPath);
        try {
            ZKClient.getInstance().getCuratorClient()
                    .create().creatingParentsIfNeeded()
                    .forPath(appStatusPath,
                            Utils.serialize(
                                    AppStatus.NEW_DEPLOYMENT_REQ_RECEIVED));
        } catch (Exception e) {
            LOGGER.error("Could not update status");
            return false;
        }


        //STEP 1c. Verify topology. (TODO)

        return true;
    }

    /**
     * Post-transition activities (like move app to archive etc.).
     *
     * @param mapping Updated Resource Mapping.
     * @return True if postTransition was successful, false otherwise.
     */
    @Override
    public final boolean postTransition(final ResourceMapping mapping) {
        //No Post transition.
        return true;
    }

    /**
     * Transition specific update resource mapping function.
     * @param appName the applicaiton name.
     * @param app The TFloeApp applicaiton object.
     * @param currentMapping Current Resource mapping.
     * @param args transaction specific arguments
     * @return updated resource mapping based on the transition.
     */
    @Override
    public final ResourceMapping updateResourceMapping(
            final String appName,
            final TFloeApp app,
            final ResourceMapping currentMapping,
            final Map<String, Object> args) {
        ResourceMapping mapping = ResourceManagerFactory.getResourceManager()
                .getInitialMapping(appName, app);

        return mapping;
    }

    /**
     * @return gets the name of the transaction.
     */
    @Override
    public final String getName() {
        return "StartAppTransition";
    }
}
