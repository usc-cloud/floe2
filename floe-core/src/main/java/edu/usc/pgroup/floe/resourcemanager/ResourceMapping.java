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
import edu.usc.pgroup.floe.thriftgen.TEdge;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.thriftgen.TPellet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Resource mapping class that stores the mapping from Pellets to Pellet
 * Instances to Containers (flakeMap).
 * @author kumbhare
 */
public class ResourceMapping implements Serializable {


    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ResourceMapping.class);

    /**
     * Floe Application.
     * (Note: Although this is thrift object, it is java serializable and
     * that is what we use for coordination through zookeeper.)
     */
    private final TFloeApp floeApp;

    /**
     * Application's unique name.
     */
    private final String appName;

    /**
     * Map from container id to a map of flakeMap (from pellet id to flake).
     */
    private Map<String, ContainerInstance> containerMap;

    /**
     * A redundant map to store a mapping from pid to a list of all flakes
     * across containers.
     */
    private Map<String, List<FlakeInstance>> pidFlakeMap;

    /**
     * Resource mapping delta to be used during runtime adaptation.
     */
    private ResourceMappingDelta mappingDelta;

    /**
     * Resource map for a given application.
     * @param name Application's name.
     * @param app the Application object received from the user's submit
     *            command.
     *
     *
     */
    public ResourceMapping(final String name, final TFloeApp app) {
        this.appName = name;
        this.floeApp = app;
        this.containerMap = new HashMap<>();
        this.pidFlakeMap = new HashMap<>();
        this.mappingDelta = null;
    }



    /**
     * @return the name of the application jar to be downloaded from the
     * coordinator.
     */
    public final String getApplicationJarPath() {
        return floeApp.get_jarPath();
    }

    /**info
     * Create a new instance of the given pellet on the given container.
     * @param pelletId pellet id.
     * @param container container info object.
     */
    public final void createNewInstance(final String pelletId,
                                  final ContainerInfo container) {

        ContainerInstance containerInstance = null;
        if (!containerMap.containsKey(container.getContainerId())) {
            containerInstance = new ContainerInstance(
                    container.getContainerId(), container);
            containerMap.put(container.getContainerId(),
                    containerInstance);
        } else {
            containerInstance = containerMap.get(
                    container.getContainerId());
        }

        FlakeInstance fl = containerInstance.getFlake(pelletId);
        if (fl == null) {
            fl = containerInstance.createFlake(pelletId);
            if (mappingDelta != null) {
                mappingDelta.flakeAdded(fl);
            }
        }

        List<FlakeInstance> pidFlakes = null;
        if (pidFlakeMap.containsKey(pelletId)) {
            pidFlakes = pidFlakeMap.get(pelletId);
        } else {
            pidFlakes = new ArrayList<>();
            pidFlakeMap.put(pelletId, pidFlakes);
        }

        pidFlakes.add(fl);

        fl.createPelletInstance();
        if (mappingDelta != null) {
            mappingDelta.flakeUpdated(fl,
                    ResourceMappingDelta.UpdateType.InstanceAdded);
        }
    }


    /**
     * Removes an instance of the pellet from the given container.
     * @param pelletName the pellet name/id.
     * @param flakeInstance A particular flake instance from which the
     *                      instance should be removed.
     */
    public final void removePelletInstance(
            final String pelletName,
            final FlakeInstance flakeInstance) {

        flakeInstance.removePelletInstance();
        if (mappingDelta != null) {
            mappingDelta.flakeUpdated(flakeInstance,
                    ResourceMappingDelta.UpdateType.InstanceRemoved);
        }

        if (flakeInstance.getNumPelletInstances() == 0) {
            ContainerInstance containerInstance = null;
            if (!containerMap.containsKey(flakeInstance.getContainerId())) {
                LOGGER.error("Invalid configuration. The Flake {} not found "
                                + "on container {}",
                        flakeInstance.getCorrespondingPelletId(),
                        flakeInstance.getContainerId());
                return;
            } else {
                containerInstance = containerMap.get(
                        flakeInstance.getContainerId());
            }

            if (containerInstance != null) {
                containerInstance.removeFlake(
                        flakeInstance.getCorrespondingPelletId());
                if (mappingDelta != null) {
                    mappingDelta.flakeRemoved(flakeInstance);
                }
            } else {
                LOGGER.error("Invalid configuration. The Flake {} not found "
                                + "on container {}",
                        flakeInstance.getCorrespondingPelletId(),
                        flakeInstance.getContainerId());
                return;
            }
        }
    }

    /**
     * Switch Alternate for given pellet to the given alternate.
     * @param pelletName the pellet name/id.
     * @param alternateName the alternate name to switch to.
     * @return true if the given alternate exists for the pellet,
     * false otherwise.
     */
    public final boolean switchAlternate(final String pelletName,
                                         final String alternateName) {


        List<FlakeInstance> pidFlakes = null;
        if (pidFlakeMap.containsKey(pelletName)) {
            pidFlakes = pidFlakeMap.get(pelletName);
        } else {
            //Pellet not found.
            return false;
        }

        TPellet pellet = floeApp.get_pellets().get(pelletName);
        if (pellet == null) {
            return false;
        }

        if (!pellet.get_alternates().containsKey(alternateName)) {
            return false;
        }

        pellet.set_activeAlternate(alternateName);

        if (mappingDelta != null) {
            for (FlakeInstance fl : pidFlakes) {
                mappingDelta.flakeUpdated(fl,
                        ResourceMappingDelta.UpdateType.ActiveAlternateChanged);
            }
        }
        return true;
    }

    /**
     * Gets the list of all preceding flakes to which the given flake should
     * connect to.
     * @param pid the pellet id.
     * @return list of all preceding flakes to which the given flake should.
     */
    public final List<FlakeInstance> getPrecedingFlakes(final String pid) {

        List<FlakeInstance> precedingFlakes = new ArrayList<>();


        TPellet tPellet = floeApp.get_pellets().get(pid);

        for (TEdge edge: tPellet.get_incomingEdges()) {
            String srcPid = edge.get_srcPelletId();
            precedingFlakes.addAll(pidFlakeMap.get(srcPid));
        }
        return precedingFlakes;
    }

    /**
     * @return The string representation of the mapping.
     */
    @Override
    public final String toString() {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append(System.lineSeparator() + "{"
                + System.lineSeparator());
        for (ContainerInstance container
                : containerMap.values()) {
            stringBuilder.append(container.toString());
        }
        stringBuilder.append("}" + System.lineSeparator());
        return stringBuilder.toString();
    }

    /**
     * @param containerId container id (as received from the heart beat)
     * @return the corresponding container instance object from the resource
     * mapping.
     */
    public final ContainerInstance getContainer(final String containerId) {
        return containerMap.get(containerId);
    }

    /**
     * @return the app name.
     */
    public final String getAppName() {
        return appName;
    }

    /**
     * Reset the mapping delta.
     */
    public final void resetDelta() {
        if (mappingDelta != null) {
            mappingDelta.reset();
        } else {
            mappingDelta = new ResourceMappingDelta(floeApp);
        }
    }

    /**
     * @return Returns the corresponding floe application
     */
    public final TFloeApp getFloeApp() {
        return floeApp;
    }

    /**
     * @return resource mapping delta since last reset.
     */
    public final ResourceMappingDelta getDelta() {
        return mappingDelta;
    }


    /**
     * @param pelletName the pellet name/id.
     * @return the list of flake instances running atleast one instance of
     * the pellet.
     */
    public final List<FlakeInstance> getFlakeInstancesForPellet(
            final String pelletName) {
        if (pidFlakeMap.containsKey(pelletName)) {
            return pidFlakeMap.get(pelletName);
        } else {
            return null;
        }
    }

    /**
     * Internal container instance class.
     */
    public class ContainerInstance implements Serializable {

        /**
         * Container's id.
         */
        private final String containerId;

        /**
         * The container info object.
         */
        private final ContainerInfo containerInfo;

        /**
         * the next available port.
         */
        private int nextPort;

        /**
         * Map from pellet id to flake instance on this container.
         */
        private Map<String, FlakeInstance> flakeMap;

        /**
         * Constructor.
         * @param cid container's id
         * @param cInfo the containerInfo object received from the heartbeat.
         */
        public ContainerInstance(final String cid, final ContainerInfo cInfo) {
            this.containerId = cid;
            this.containerInfo = cInfo;
            this.nextPort = containerInfo.getPortRangeStart();
            this.flakeMap = new HashMap<>();
        }

        /**
         * @return returns the next available port for a flake to listen on.
         */
        private int getNextAvailablePort() {
            return nextPort++;
        }


        /**
         * Creates a new flake instance.
         * @param pid create flake for the given pid in the current container.
         * @return the newly created flake instance.
         */
        private FlakeInstance createFlake(final String pid) {

            TPellet tPellet = floeApp.get_pellets().get(pid);

            int numPorts = Math.max(1, tPellet.get_outgoingEdges().size());
            int[] flPorts = new int[numPorts];
            for (int i = 0; i < numPorts; i++) {
                flPorts[i] = getNextAvailablePort();
            }

            FlakeInstance flakeInstance = new FlakeInstance(pid, containerId,
                    containerInfo.getHostnameOrIpAddr(), flPorts);
            flakeMap.put(pid, flakeInstance);
            return  flakeInstance;
        }


        /**
         * Removes the empty flake from the container.
         * @param pid create flake for the given pid in the current container.
         * @return true if the flake was empty and removed,
         * false otherwise.
         */
        public final boolean removeFlake(final String pid) {
            FlakeInstance flInstance = flakeMap.get(pid);
            if (flInstance.getNumPelletInstances() > 0) {
                LOGGER.warn("Flake not empty");
                return false;
            }

            flakeMap.remove(pid);
            return true;
        }

        /**
         * @param pid pid for the flake to return.
         * @return the flake instance if it exists, null otherwise.
         */
        public final FlakeInstance getFlake(final String pid) {
            if (flakeMap.containsKey(pid)) {
                return flakeMap.get(pid);
            }
            return null;
        }

        /**
         * @return all the flakes that should be deployed on this container.
         */
        public final Map<String, FlakeInstance> getFlakes() {
            return flakeMap;
        }

        /**

         * @return string rep. of flakes on the container.
         */
        @Override
        public final String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(containerId + ": ");
            stringBuilder.append("[");
            for (FlakeInstance flake: flakeMap.values()) {
                stringBuilder.append(flake.toString());
            }
            stringBuilder.append("]" + System.lineSeparator());
            return stringBuilder.toString();
        }
    }

    /**
     * Flake Instance info class.
     */
    public class FlakeInstance implements Serializable {

        /**
         * Pellet id for which this flake runs the instances.
         */
        private final String pelletId;

        /**
         * Host name or ip address of the container on which this flake will
         * be initialized.
         */
        private final String host;

        /**
         * The ports on which this flake should listen for connections from
         * succeeding pellets.
         */
        private final Map<String, Integer> listeningPorts;

        /**
         * Container id to which this flake belongs.
         */
        private final String containerId;

        /**
         * Number of pellet instance on this flake.
         */
        private int numPelletInstances;

        /**
         * Constructor.
         * @param pid Pelletid for which this flake will run the instances.
         * @param cid Container id to which this flake belongs.
         * @param hostnameOrIpAddr Host name or ip address of the container
         * @param flPorts ports on which this flake should listen for
         */
        public FlakeInstance(final String pid, final String cid,
                             final String hostnameOrIpAddr,
                             final int[] flPorts) {
            this.containerId = cid;
            this.pelletId = pid;
            this.host = hostnameOrIpAddr;
            this.listeningPorts = new HashMap<>();
            this.numPelletInstances = 0;

            TPellet tPellet = floeApp.get_pellets().get(pid);

            if (tPellet.get_outgoingEdges().size() > 0) {
                int i = 0;
                for (TEdge edge : tPellet.get_outgoingEdges()) {
                    listeningPorts.put(edge.get_destPelletId(), flPorts[i++]);
                }
            } else {
                listeningPorts.put("out", flPorts[0]);
            }

        }


        /**
         * @param pid pid of the succeeding pellet.
         * @return the port assigned to this pellet to connect to.
         */
        public final int getAssignedPort(final String pid) {
            return listeningPorts.get(pid);
        }

        /**
         * Create a new pellet instance on this flake.
         */
        public final void createPelletInstance() {
            numPelletInstances++;
        }

        /**
         * Removes one pellet instance from the flake.
         */
        public final void removePelletInstance() {

            if (numPelletInstances > 0) {
                numPelletInstances--;
            } else {
                LOGGER.error("NumPeleltInstance: " + numPelletInstances);
                throw new IllegalArgumentException("The flake has zero "
                        + "pellets running on it.");
            }
        }
        /**
         * @return the host on which this flake runs (this can be used by
         * other flakeMap to connect to this flake).
         */
        public final String getHost() {
            return host;
        }

        /**
         * @return the ports on which this flake should listen for connections.
         */
        public final Integer[] getListeningPorts() {
            Integer[] arr = new Integer[listeningPorts.size()];
            return listeningPorts.values().toArray(arr);
        }

        /**
         * @return string rep. of all pellet instances on the flake.
         */
        @Override
        public final String toString() {
            StringBuilder builder = new StringBuilder();
            builder.append("(");
            builder.append(pelletId + ":");
            builder.append("[");
            for (int p: listeningPorts.values()) {
                builder.append(p + ", ");
            }
            if (listeningPorts.size() > 0) {
                builder.delete(builder.toString().length() - 2,
                        builder.toString().length() - 1);
            }
            builder.append("]");

            builder.append(",[");
            builder.append(numPelletInstances);
            builder.append("]");
            builder.append("); ");
            return builder.toString();
        }

        /**
         * @return the list of pellet instances to run on the flake.
         */
        public final int getNumPelletInstances() {
            return numPelletInstances;
        }

        /**
         * @return the name of the pellet that this flake runs.
         */
        public final String getCorrespondingPelletId() {
            return pelletId;
        }

        /**
         * @return container id to which this flake belongs.
         */
        public final String getContainerId() {
            return containerId;
        }
    }
}
