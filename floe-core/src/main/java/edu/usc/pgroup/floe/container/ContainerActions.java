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

package edu.usc.pgroup.floe.container;

import edu.usc.pgroup.floe.client.FloeClient;
import edu.usc.pgroup.floe.flake.FlakeInfo;
import edu.usc.pgroup.floe.resourcemanager.ResourceMapping;
import edu.usc.pgroup.floe.resourcemanager.ResourceMappingDelta;
import edu.usc.pgroup.floe.thriftgen.TPellet;
import edu.usc.pgroup.floe.utils.RetryLoop;
import edu.usc.pgroup.floe.utils.RetryPolicyFactory;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author kumbhare
 */
public final class ContainerActions {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ContainerActions.class);


    /**
     * Hiding default constructor.
     */
    private ContainerActions() {

    }

    /**
     * Function to create flakes within the container from the resource mapping.
     * @param mapping Current Resource mapping
     * @throws Exception if there is an error communicating with ZK or while
     * launching flakes.
     */
    public static void createFlakes(final ResourceMapping mapping)
            throws Exception {

        String appName = mapping.getAppName();

        String applicationJar = mapping.getApplicationJarPath();

        if (applicationJar != null) {
            try {

                String downloadLocation
                        = Utils.getContainerJarDownloadPath(appName,
                                                            applicationJar);

                LOGGER.info("Downloading: " + applicationJar);
                FloeClient.getInstance().downloadFileSync(applicationJar,
                        downloadLocation);
                LOGGER.info("Finished Downloading: " + applicationJar);
            } catch (Exception e) {
                LOGGER.warn("No application jar specified. It should work"
                              + " still work for inproc testing. Exception: {}",
                        e);
            }
        }

        String containerId = ContainerInfo.getInstance().getContainerId();

        Map<String, ResourceMapping.FlakeInstance> flakes = null;

        if (mapping.getDelta() == null) {
            ResourceMapping.ContainerInstance container
                    = mapping.getContainer(containerId);
            if (container != null) {
                flakes = container.getFlakes();
            } else {
                LOGGER.warn("No flakes for this container.");
            }
        } else {
            Map<String, ResourceMappingDelta.FlakeInstanceDelta>
                    addedflakeDeltas
                    = mapping.getDelta().getNewlyAddedFlakes(containerId);

            if (addedflakeDeltas != null) {
                flakes = new HashMap<>();

                for (Map.Entry<String, ResourceMappingDelta.FlakeInstanceDelta>
                        fd : addedflakeDeltas.entrySet()) {
                    flakes.put(fd.getKey(), fd.getValue().getFlakeInstance());
                }
            } else {
                LOGGER.warn("No new flakes for this container.");
            }
        }

        //Create and wait for flakes to respond.
        if (flakes != null) {
            ContainerUtils.createFlakes(
                    mapping.getAppName(),
                    mapping.getApplicationJarPath(),
                    containerId,
                    flakes);
        }
    }


    /**
     * Function to terminate flakes if any as determined by the Resource
     * Mapping. Do not blindly terminate all flakes in the container for the
     * given app.
     * Precondition: Flake should not have any
     * pellets running.
     * @param mapping Current Resource mapping
     * @throws Exception if there is an error communicating with ZK or while
     * launching flakes.
     */
    public static void terminateFlakes(final ResourceMapping mapping)
            throws Exception {

        String containerId = ContainerInfo.getInstance().getContainerId();

        if (mapping.getDelta() != null) {
            Map<String,
                    ResourceMappingDelta.FlakeInstanceDelta> flakeDeltasRemoved
                    = mapping.getDelta().getRemovedFlakes(containerId);

            if (flakeDeltasRemoved != null && flakeDeltasRemoved.size() > 0) {
                ContainerUtils.terminateFlakes(mapping, flakeDeltasRemoved);
            }
        }
    }

    /**
     * Function to increment or decrement the pellets for certain flakes based
     * on the resource mapping.
     * @param mapping Current Resource mapping.
     * @throws Exception if there is an error communicating with ZK or while
     * add/removing pellets.
     */
    public static void increaseOrDecreasePellets(
            final ResourceMapping mapping) throws Exception {

        String containerId = ContainerInfo.getInstance().getContainerId();

        Map<String, ResourceMapping.FlakeInstance> flakes = null;

        Map<String, Integer> pelletsToIncrOrDecr = new HashMap<>();

        if (mapping.getDelta() == null) {
            ResourceMapping.ContainerInstance container
                    = mapping.getContainer(containerId);
            if (container != null) {
                flakes = container.getFlakes();
            } else {
                LOGGER.warn("No pellets for this container.");
            }

            for (Map.Entry<String, ResourceMapping.FlakeInstance> flake
                    : flakes.entrySet()) {
                pelletsToIncrOrDecr.put(flake.getKey(),
                        flake.getValue().getNumPelletInstances());
            }
        } else {
            Map<String, ResourceMappingDelta.FlakeInstanceDelta>
                    addedflakeDeltas = new HashMap<>();


            if (mapping.getDelta().getNewlyAddedFlakes(containerId) != null) {
                addedflakeDeltas.putAll(
                        mapping.getDelta().getNewlyAddedFlakes(containerId));
            }

            if (mapping.getDelta().getUpdatedFlakes(containerId) != null) {
                addedflakeDeltas.putAll(
                        mapping.getDelta().getUpdatedFlakes(containerId));
            }

            if (mapping.getDelta().getRemovedFlakes(containerId) != null) {
                addedflakeDeltas.putAll(
                        mapping.getDelta().getRemovedFlakes(containerId));
            }


            if (addedflakeDeltas != null && addedflakeDeltas.size() > 0) {
                flakes = new HashMap<>();

                for (Map.Entry<String, ResourceMappingDelta.FlakeInstanceDelta>
                        fd : addedflakeDeltas.entrySet()) {
                    flakes.put(fd.getKey(), fd.getValue().getFlakeInstance());

                    int diffPellets = fd.getValue().getNumInstancesAdded()
                            - fd.getValue().getNumInstancesRemoved();

                    pelletsToIncrOrDecr.put(fd.getKey(), diffPellets);
                }
            } else {
                LOGGER.warn("No new pellets for this container.");
            }
        }

        try {

            for (Map.Entry<String, ResourceMapping.FlakeInstance> flakeEntry
                    : flakes.entrySet()) {

                final String pid = flakeEntry.getKey();

                FlakeInfo info = RetryLoop.callWithRetry(RetryPolicyFactory
                                .getDefaultPolicy(),
                        new Callable<FlakeInfo>() {
                            @Override
                            public FlakeInfo call() throws Exception {
                                return FlakeMonitor.getInstance()
                                        .getFlakeInfo(pid);
                            }
                        });
                LOGGER.info("Found Flake:{}", info.getFlakeId());

                ResourceMapping.FlakeInstance flakeInstance
                        = flakeEntry.getValue();

                TPellet pellet = mapping.getFloeApp().get_pellets()
                        .get(flakeInstance.getCorrespondingPelletId());

                byte[] activeAlternate = pellet.get_alternates().get(
                        pellet.get_activeAlternate()
                ).get_serializedPellet();

                int diffPellets = pelletsToIncrOrDecr.get(pid);
                LOGGER.info("Creating {} instances.", diffPellets);

                if (diffPellets > 0) {
                    //TO INCREMENT.
                    LOGGER.info("Incrementing pellet instances for flake: "
                            + "{} by {}", info.getFlakeId(), diffPellets);
                    for (int i = 0; i < diffPellets; i++) {
                        ContainerUtils.sendIncrementPelletCommand(
                                info.getFlakeId(),
                                activeAlternate
                        );
                    }
                } else if (diffPellets < 0) {
                    //TO Decrement.
                    diffPellets = Math.abs(diffPellets);
                    LOGGER.info("Decrementing pellet instances for flake: "
                            + "{} by {}", info.getFlakeId(), diffPellets);
                    for (int i = 0; i < diffPellets; i++) {
                        ContainerUtils.sendDecrementPelletCommand(
                                info.getFlakeId()
                        );
                    }
                }
            }
        } catch (Exception ex) {
            LOGGER.error("Could not update flake.");
        }
    }



    /**
     * Function to increment or decrement the pellets for certain flakes based
     * on the resource mapping.
     * @param mapping Current Resource mapping.
     * @throws Exception if there is an error communicating with ZK or while
     * add/removing pellets.
     */
    public static void updateFlakeConnections(
            final ResourceMapping mapping) throws Exception {

        //Two things to consider.
        //First.. For new flakes. Connect to ALL predecessors.
        //Second.. For all flakes, Connect to NEW predecessor Flakes.


        //First.. new flakes.
        String containerId = ContainerInfo.getInstance().getContainerId();
        Map<String, ResourceMapping.FlakeInstance> flakes = null;

        if (mapping.getDelta() == null) {
            ResourceMapping.ContainerInstance container
                    = mapping.getContainer(containerId);
            if (container != null) {
                flakes = container.getFlakes();
            } else {
                LOGGER.warn("No flakes for this container.");
            }
        } else {
            Map<String, ResourceMappingDelta.FlakeInstanceDelta>
                    addedflakeDeltas
                    = mapping.getDelta().getNewlyAddedFlakes(containerId);

            if (addedflakeDeltas != null) {
                flakes = new HashMap<>();

                for (Map.Entry<String, ResourceMappingDelta.FlakeInstanceDelta>
                        fd : addedflakeDeltas.entrySet()) {
                    flakes.put(fd.getKey(), fd.getValue().getFlakeInstance());
                }
            } else {
                LOGGER.warn("No new flakes for this container.");
            }
        }

        if (flakes != null) {
            Map<String, FlakeInfo> pidToFidMap
                    = FlakeMonitor.getInstance().getFlakes();

            for (Map.Entry<String, ResourceMapping.FlakeInstance> flakeEntry
                    : flakes.entrySet()) {

                String pid = flakeEntry.getKey();
                List<ResourceMapping.FlakeInstance> preds
                        = mapping
                        .getPrecedingFlakes(pid);

                for (ResourceMapping.FlakeInstance pred : preds) {
                    int assignedPort = pred.getAssignedPort(pid);
                    int backPort = pred.getAssignedBackPort(pid);
                    String host = pred.getHost();
                    ContainerUtils.sendConnectCommand(
                            pidToFidMap.get(pid).getFlakeId(),
                            host, assignedPort, backPort);
                }
            }
        }

        //Second. For all existing flakes, connect to new predecessors.
        Map<String, FlakeInfo> runningFlakes
                = FlakeMonitor.getInstance().getFlakes();

        for (Map.Entry<String, FlakeInfo> entry
                : runningFlakes.entrySet()) {

            //THIS IS REQUIRED ONLY IF THIS IS NOT A NEWLY CREATED PELLET
            // . BECAUSE THE launchAndInitialize function will take care
            // of connections.
            //SO CHECK IF THIS PELLET IS PRESENT IN THE NEWLY ADDED LIST.
            if (flakes != null
                    && flakes.containsKey(entry.getKey())) {
                continue;
            }

            String pelletName = entry.getKey();
            FlakeInfo flakeInfo = entry.getValue();

            //get DELTA ONLY those predecessor flakes that have been
            // added for this pellet.
            List<ResourceMappingDelta.FlakeInstanceDelta> newPredFlakes
                    = mapping.getDelta()
                    .getNewlyAddedPrecedingFlakes(pelletName);

            if (newPredFlakes != null) {
                for (ResourceMappingDelta.FlakeInstanceDelta predDelta
                        : newPredFlakes) {
                    ResourceMapping.FlakeInstance pred
                            = predDelta.getFlakeInstance();
                    int assignedPort = pred.getAssignedPort(pelletName);
                    int backPort = pred.getAssignedBackPort(pelletName);
                    String host = pred.getHost();
                    ContainerUtils.sendConnectCommand(
                            flakeInfo.getFlakeId(),
                            host, assignedPort, backPort);
                }
            } else {
                LOGGER.info("No new pred. flakes. So no new connections.");
            }

        }
    }

    /**
     * Utility function to check if the current container has any updates to
     * make.
     * @param mapping Current Resource mapping.
     * @return true if updates are required, false other wise.
     */
    public static boolean isContainerUpdated(final ResourceMapping mapping) {
        String containerId = ContainerInfo.getInstance().getContainerId();

        if (mapping.getDelta() != null) {
            if (mapping.getDelta().isContainerUpdated(containerId)) {
                return true;
            } else {
                LOGGER.warn("No updates for this container.");
                return false;
            }
        } else {
            ResourceMapping.ContainerInstance container
                    = mapping.getContainer(containerId);
            if (container == null) {
                LOGGER.warn("No flakes for this container.");
                return false;
            } else {
                return true;
            }
        }
    }
}
