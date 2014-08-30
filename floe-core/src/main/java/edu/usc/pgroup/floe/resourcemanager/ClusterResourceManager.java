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
import edu.usc.pgroup.floe.thriftgen.AlternateNotFoundException;
import edu.usc.pgroup.floe.thriftgen.ScaleDirection;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.thriftgen.TPellet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * @author kumbhare
 */
public class ClusterResourceManager extends ResourceManager {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ResourceMapping.class);

    /**
     * default constructor.
     */
    public ClusterResourceManager() {

    }

    /**
     * Gets the initial resource mapping for the Floe Application.
     * @param appName Application name.
     * @param app The fully configured floe application,
     *            with optional estimated input data rates and processing
     *            costs for different pellets.
     * @return returns an initial mapping from pellet instances to containers.
     */
    @Override
    public final ResourceMapping getInitialMapping(
            final String appName,
            final TFloeApp app) {
        ResourceMapping mapping = new ResourceMapping(appName, app);

        List<ContainerInfo> containers = getAvailableContainersWithRetry();

        if (containers == null || containers.size() <= 0) {
            LOGGER.error("Error occurred while acquiring and all attempts to "
                    + "retry have failed.");
            return null;
        }

        int i = 0;
        for (Map.Entry<String, TPellet> pelletEntry
                : app.get_pellets().entrySet()) {

            int numInstances = 1;

            if (pelletEntry.getValue().get_parallelism() > 0) {
                numInstances = pelletEntry.getValue().get_parallelism();
            }

            for (int cnt = 0; cnt < numInstances; cnt++) {
                mapping.createNewInstance(pelletEntry.getKey(),
                        containers.get(i++));
                if (i == containers.size()) {
                    i = 0;
                }
            }
        }
        return  mapping;
    }


    /**
     * Updates the resource mapping based on the current floe application
     * status (and perf. numbers)
     *
     * @param app     The current application status. (the perf numbers are not
     *                included, which can be fetched from the Zookeeper)
     * @param current Current resource mapping.
     * @return will return ONLY the updated (added/removed/changed) mapping
     * parameters. anything not included should remain the same.
     */
    @Override
    public final ResourceMapping updateResourceMapping(
            final TFloeApp app,
            final ResourceMapping current) {
        return null;
    }

    /**
     * Scales the given pellet up or down for the given number of instances.
     *
     * @param current    the current resource mapping.
     * @param direction  direction of scaling.
     * @param pelletName name of the pellet to scale.
     * @param count      the number of instances to scale up/down.
     * @return the updated resource mapping with the ResourceMappingDelta set
     * appropriately.
     */
    @Override
    public final ResourceMapping scale(final ResourceMapping current,
                                 final ScaleDirection direction,
                                 final String pelletName, final int count) {

        List<ContainerInfo> containers = getAvailableContainersWithRetry();

        if (containers == null || containers.size() <= 0) {
            LOGGER.error("Error occurred while acquiring and all attempts to "
                    + "retry have failed.");
            return null;
        }

        //TODO: Order containers w.r.t availability.

        int cindex = 0;

        current.resetDelta();
        for (int i = 0; i < count; i++) {
            current.createNewInstance(pelletName, containers.get(cindex++));
            if (cindex == containers.size()) {
                cindex = 0;
            }
        }

        return current;
    }

    /**
     * Switches the active alternate for the pellet.
     *
     * @param currentMapping the current resource mapping (this is for a
     *                       particular app, so no need floe app parameter).
     * @param pelletName     name of the pellet to switch alternate for.
     * @param alternateName  the name of the alternate to switch to.
     * @return the updated resource mapping with the ResourceMappingDelta set
     * appropriately.
     * @throws edu.usc.pgroup.floe.thriftgen.AlternateNotFoundException if
     * the alternate is not found for the given pellet.
     */
    @Override
    public final ResourceMapping switchAlternate(
            final  ResourceMapping currentMapping,
            final String pelletName,
            final String alternateName) throws AlternateNotFoundException {

        currentMapping.resetDelta();
        if (!currentMapping.switchAlternate(pelletName, alternateName)) {
            LOGGER.error("The given alternate: {} for pellet: {} does not "
                            + "exist.", pelletName, alternateName);
            throw new AlternateNotFoundException("The given alternate: "
                    + pelletName + " for pellet: " + alternateName + " does "
                    + "not exist.");
        }
        return currentMapping;
    }
}
