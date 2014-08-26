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

import edu.usc.pgroup.floe.container.ContainerInfo;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;

import java.util.List;

/**
 * Resource manager interface.
 * @author kumbhare
 */
public abstract class ResourceManager {


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
     * @return will return ONLY the updated (added/removed/changed) mapping
     * parameters. anything not included should remain the same.
     */
    public abstract ResourceMapping updateResourceMapping(TFloeApp app,
                                          ResourceMapping current);
}
