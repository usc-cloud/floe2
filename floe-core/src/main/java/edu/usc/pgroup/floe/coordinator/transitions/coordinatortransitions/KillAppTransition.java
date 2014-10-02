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
import java.util.UUID;

/**
 * @author kumbhare
 */
public class KillAppTransition extends BaseAppTransition {
    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(KillAppTransition.class);


    /**
     * @return gets the name of the transaction.
     */
    @Override
    public final String getName() {
        return "KillTransaction";
    }

    /**
     * Pre-transition activities (like verification of topology,
     * app exists etc.).
     *
     * @param appName the applicaiton name.
     * @param mapping Current Resource Mapping. (null for a new deployment)
     * @return True if preTransition was successful and the transition itself
     * should be executed. False implies that there are an error and the
     * transition should be skipped.
     */
    @Override
    public final boolean preTransition(final String appName,
                                       final ResourceMapping mapping) {
        try {
            if (!ZKUtils.appExists(appName)) {
                LOGGER.error("Application does not exist.");
                return false;
            }
        } catch (Exception e) {
            LOGGER.error("Could not contact ZK, {}", e);
            return false;
        }


        String appPath = ZKUtils.getApplicationPath(appName);

        String appStatusPath = ZKUtils.getApplicationStatusPath(appName);

        LOGGER.info("App Path to store the configuration:" + appPath);
        try {
            ZKUtils.setAppStatus(appName, AppStatus.NEW_REQ_RECEIVED);

        } catch (Exception e) {
            LOGGER.error("Could not update status");
            return false;
        }

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

        String appName = mapping.getAppName();

        //Move the app from running to terminated.
        String terminatedAppPath = ZKUtils
                .getApplicationTerminatedInfoPath(appName + "-"
                        + UUID.randomUUID());
        //Copy data to terminated section. Currently it is just the RM but
        // later we will have much more.

        byte[] serializedRM = Utils.serialize(mapping);

        try {
            ZKClient.getInstance().getCuratorClient().create()
                    .creatingParentsIfNeeded()
                    .forPath(terminatedAppPath, serializedRM);

        ZKUtils.setAppStatus(appName, AppStatus.TERMINATED);
        LOGGER.info("App terminated. Moving to archive.");
//
        //Now delete from the aaps path.
        String appPath = ZKUtils.getApplicationPath(appName);
        ZKClient.getInstance().getCuratorClient().delete()
                .deletingChildrenIfNeeded().forPath(appPath);

        } catch (Exception e) {
            return false;
        }

        return true;
    }

    /**
     * Transition specific update resource mapping function.
     *
     * @param appName        the applicaiton name.
     * @param app            The TFloeApp applicaiton object.
     * @param currentMapping Current Resource mapping.
     * @param args           transaction specific arguments
     * @return updated resource mapping based on the transition.
     */
    @Override
    public final ResourceMapping updateResourceMapping(
            final String appName,
            final TFloeApp app,
            final ResourceMapping currentMapping,
            final Map<String, Object> args) {

        LOGGER.info("Kill all pellet instances (and flakes)");
        ResourceMapping newMapping = ResourceManagerFactory.getResourceManager()
                .kill(currentMapping);

        return newMapping;
    }
}
