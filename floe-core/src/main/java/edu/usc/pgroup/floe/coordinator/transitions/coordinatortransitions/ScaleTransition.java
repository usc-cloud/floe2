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
import edu.usc.pgroup.floe.thriftgen.ScaleDirection;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author kumbhare
 */
public class ScaleTransition extends BaseAppTransition {
    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ScaleTransition.class);

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
    public final boolean preTransition(
            final String appName,
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
        //No Post transition required.
        return false;
    }

    /**
     * Transition specific update resource mapping function.
     *
     * @param appName        the applicaiton name.
     * @param app            The TFloeApp applicaiton object.
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
        ScaleDirection direction = (ScaleDirection) args.get("direction");
        String pelletName = (String) args.get("pelletName");
        Integer count = (Integer) args.get("count");

        ResourceMapping newMapping = ResourceManagerFactory.getResourceManager()
                .scale(currentMapping, direction, pelletName, count);

        return newMapping;
    }

    /**
     * @return gets the name of the transaction.
     */
    @Override
    public final String getName() {
        return "ScaleTransition";
    }
}
