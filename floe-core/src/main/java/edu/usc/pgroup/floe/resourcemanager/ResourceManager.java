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

package edu.usc.pgroup.floe.resourcemanager;

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.container.ContainerInfo;
import edu.usc.pgroup.floe.thriftgen.AlternateNotFoundException;
import edu.usc.pgroup.floe.thriftgen.ScaleDirection;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.utils.RetryLoop;
import edu.usc.pgroup.floe.utils.RetryPolicy;
import edu.usc.pgroup.floe.utils.RetryPolicyFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Callable;

/**
 * Resource manager interface.
 * @author kumbhare
 */
public abstract class ResourceManager {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ResourceManager.class);

    /**
     * Cluster resource monitor. This monitors the the containers which are
     * already started and registered.
     */
    private final ResourceMonitor clusterResourceMonitor;

    /**
     * Default constructor.
     */
    ResourceManager() {
        clusterResourceMonitor = ResourceMonitor.getInstance();
    }

    /**
     * @return the list of containers who have registered with the resource
     * manager.
     */
    protected final List<ContainerInfo> getAvailableContainers() {
        return clusterResourceMonitor.getAvailableContainers();
    }

    /**
     * @return the list of containers who have registered with the resource
     * manager.
     */
    protected final List<ContainerInfo> getAvailableContainersWithRetry() {
        List<ContainerInfo> containers = null;


        RetryPolicy retryPolicy = RetryPolicyFactory.getRetryPolicy(
                FloeConfig.getConfig().getString(ConfigProperties
                        .RESOURCEMANAGER_RETRYPOLICY),
                FloeConfig.getConfig().getStringArray(ConfigProperties
                        .RESOURCEMANAGER_RETRYPOLICY_ARGS)
        );

        try {
            containers = RetryLoop.callWithRetry(retryPolicy,
                    new Callable<List<ContainerInfo>>() {
                        @Override
                        public List<ContainerInfo> call() throws Exception {
                            List<ContainerInfo> containers
                                    = getAvailableContainers();
                            if (containers == null || containers.size() <= 0) {
                                throw new Exception(
                                        "No Containers registered yet");
                            }
                            return containers;
                        }
                    }
            );
        } catch (Exception e) {
            LOGGER.error("Error occurred while acquiring and all attempts to "
                    + "retry have failed. Exception: {} ", e);
            return null;
        }

        return  containers;
    }

    /**
     * Gets the initial resource mapping for the Floe Application.
     * @param appName Application name.
     * @param app The fully configured floe application,
     *            with optional estimated input data rates and processing
     *            costs for different pellets.
     * @return returns an initial mapping from pellet instances to containers.
     */
    public abstract ResourceMapping getInitialMapping(String appName,
                                                      TFloeApp app);


    /**
     * Updates the resource mapping based on the current floe application
     * status (and perf. numbers)
     * @param app The current application status. (the perf numbers are not
     *            included, which can be fetched from the Zookeeper)
     * @param current Current resource mapping.
     * @return the updated resource mapping with the ResourceMappingDelta set
     * appropriately.
     *
     */
    public abstract ResourceMapping updateResourceMapping(TFloeApp app,
                                          ResourceMapping current);


    /**
     * Scales the given pellet up or down for the given number of instances.
     * @param current the current resource mapping.
     * @param direction direction of scaling.
     * @param pelletName name of the pellet to scale.
     * @param count the number of instances to scale up/down.
     * @return the updated resource mapping with the ResourceMappingDelta set
     * appropriately.
     */
    public abstract ResourceMapping scale(ResourceMapping current,
                                          ScaleDirection direction,
                                          String pelletName,
                                          int count);

    /**
     * Switches the active alternate for the pellet.
     * @param currentMapping the current resource mapping (this is for a
     *                       particular app, so no need floe app parameter).
     * @param pelletName name of the pellet to switch alternate for.
     * @param alternateName the name of the alternate to switch to.
     * @return the updated resource mapping with the ResourceMappingDelta set
     * appropriately.
     * @throws edu.usc.pgroup.floe.thriftgen.AlternateNotFoundException if
     * the alternate is not found for the given pellet.
     */
    public abstract ResourceMapping switchAlternate(
            ResourceMapping currentMapping,
            String pelletName,
            String alternateName) throws AlternateNotFoundException;
}
