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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

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

        for (Map.Entry<String, TPellet> pelletEntry
                : app.get_pellets().entrySet()) {

            int numInstances = 1;

            if (pelletEntry.getValue().get_parallelism() > 0) {
                numInstances = pelletEntry.getValue().get_parallelism();
            }

            //Integer tokens[] = {-598260261, 1224420058, 1077889423};
            int idx = -1;
            ContainerInfo prev = null;
            for (int cnt = 0; cnt < numInstances; cnt++) {

                ContainerInfo container = getFreeContainer(
                        pelletEntry.getKey(),
                        containers,
                        mapping);
                /*if (pelletEntry.getKey().equalsIgnoreCase("count")) {
                    LOGGER.error("Setting token for count");
                    if (prev != container) {
                        idx++;
                    }
                    prev = container;
                    LOGGER.error("Token:{}", tokens[idx].toString());
                    mapping.createNewInstance(pelletEntry.getKey(),
                            container, tokens[idx].toString());
                } else {*/

                    mapping.createNewInstance(pelletEntry.getKey(),
                            container, null);
                //}
            }
        }
        return  mapping;
    }

    /**
     * This tries to isolate pellets on different containers. So if a
     * container already contains a certain type of pellet,
     * it is preferred to keep the same type of pellet on that container.
     * @param pelletId pelletid.
     * @param mapping Current resource mapping.
     * @param containers list of available containers (from ZK).
     * @return free container for the given Pellet.
     */
    public final ContainerInfo getFreeContainer(
            final String pelletId, final List<ContainerInfo> containers,
            final ResourceMapping mapping) {

        List<ContainerInfo> feasible = new ArrayList<>();
        for (ContainerInfo container : containers) {
            //If there are no cores available, move to next container.
            if (container.getAvailableCores() == 0) {
                LOGGER.info("No availalbe cores: {}",
                        container.getContainerId());
                continue;
            }

            //If the container has a different pellet running,
            // move to next container.
            LOGGER.info("Flakes on {} : {}", container.getContainerId(),
                    container.getCurrentFlakes());
            if (container.getCurrentFlakes() != null
                    && container.getCurrentFlakes().size() > 0) {

                /*for (String cpid: container.getCurrentFlakes().keySet()) {
                    LOGGER.info("Container hosts: {}, will put {}",
                            cpid, pelletId);
                    if (!cpid.equalsIgnoreCase(pelletId)) {
                        LOGGER.info("Other pellet running: {}, expected {}",
                                container.getContainerId(), pelletId);
                        continue; //bug
                    }
                }*/
                if (!container.getCurrentFlakes().keySet().contains(pelletId)) {
                    LOGGER.info("Other pellet running: {}, expected {}",
                            container.getContainerId(), pelletId);
                    continue;
                }
            }

            //If here, it means the container has free cores.
            //Now check if during the current mapping, all cores have been used.
            //If so, move to the next container.
            int usedCores = mapping.getUsedCores(container.getContainerId());
            if (usedCores >= container.getNumCores()) {
                LOGGER.info("Resource full: {}, used:{}, available:{}",
                        container.getContainerId(), usedCores,
                        container.getNumCores());
                continue;
            }

            //Also check if the current mapping's container's flake has the
            // given pellet id.


            //THE FOLLOWING IS TO RESTRICT ONE CONTAINTER TO SINGLE PELLET
            // TYPE> WE DID THIS FOR IPDPS EXPERIMENTS. DONT NEED THIS NOW>

            /*
            ResourceMapping.ContainerInstance cmapping
                    = mapping.getContainer(container.getContainerId());
            if (cmapping != null) {
                LOGGER.info("Resource mapping, but not deployed flakes {} on "
                        + "container {}", cmapping.getFlakes(),
                        container.getContainerId());
                if (!cmapping.getFlakes().containsKey(pelletId)) {
                    continue;
                }
            }*/

            LOGGER.info("Feasible: {}", container.getContainerId());
            feasible.add(container);
        }

        //Find the container Most cores used.
        int min = -1;
        ContainerInfo best = null;
        for (ContainerInfo c: feasible) {
            int usedCores = mapping.getUsedCores(c.getContainerId());
            if (usedCores > min) {
                LOGGER.info("best: {}", c.getContainerId());
                best = c;
                min = usedCores;
            }
        }
        return best;
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

        current.resetDelta();

        if (direction == ScaleDirection.up) {
            List<ContainerInfo> containers = getAvailableContainersWithRetry();

            if (containers == null || containers.size() <= 0) {
                LOGGER.error("Error occurred while acquiring and all attempts"
                        + "to retry have failed.");
                return null;
            }

            //TODO: Order containers w.r.t availability.



            //int cindex = new Random().nextInt(containers.size());

            //Add to containers in round robin fashion.
            for (int i = 0; i < count; i++) {
                ContainerInfo container = getFreeContainer(
                                                pelletName,
                                                containers,
                                                current);
                current.createNewInstance(pelletName, container, null);
            }
        } else if (direction == ScaleDirection.down) {

            List<ResourceMapping.FlakeInstance> flakes
                    = current.getFlakeInstancesForPellet(pelletName);

            if (flakes == null || flakes.size() <= 0) {
                LOGGER.error("No flakes executing the given pellet exist.");
                return null;
            }

            LOGGER.info("Flakes:{}", flakes);
            //TODO: Order containers w.r.t availability.

            //Randomly select a flake to remove instance.
            int cindex = new Random().nextInt(flakes.size());

            //Remove from containers in round robin fashion.
            for (int i = 0; i < count; i++) {
                current.removePelletInstance(pelletName,
                        flakes.get(cindex++));
                if (cindex == flakes.size()) {
                    cindex = 0;
                }
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

    /**
     * Prepares the application to be killed by removing all pelletinstances,
     * all flakes.
     *
     * @param currentMapping current resource mapping for the application to be
     *                 killed.
     * @return the updated resource mapping with the ResourceMappingDelta set
     * appropriately.
     */
    @Override
    public final ResourceMapping kill(final  ResourceMapping currentMapping) {
        currentMapping.resetDelta();
        for (String peName
                : currentMapping.getFloeApp().get_pellets().keySet()) {
            List<ResourceMapping.FlakeInstance> flakes
                    = currentMapping.getFlakeInstancesForPellet(peName);

            if (flakes == null || flakes.size() <= 0) {
                LOGGER.warn("No flakes executing the given pellet exist.");
                continue;
            }

            LOGGER.info("Flakes:{}", flakes);


            ResourceMapping.FlakeInstance flake;
            while (flakes.size() > 0) {
                flake = flakes.get(0);
                int numInstances = flake.getNumPelletInstances();
                for (int i = 0; i < numInstances; i++) {
                    currentMapping.removePelletInstance(peName, flake);
                }
            }
        }
        return currentMapping;
    }
}
