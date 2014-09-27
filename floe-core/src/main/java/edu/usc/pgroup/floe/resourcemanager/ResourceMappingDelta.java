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


import edu.usc.pgroup.floe.thriftgen.TEdge;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.thriftgen.TPellet;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author kumbhare
 */
public class ResourceMappingDelta implements Serializable {

    /**
     * The floe application.
     */
    private final TFloeApp floeApp;


    /**
     * Redundant list of all modified containers. (since the last reset).
     */
    private final List<String> modifiedContainers;

    /**
     * Map from container id to newly added flakes (and the pellet instances
     * within that).
     */
    private final Map<String,
            Map<String, FlakeInstanceDelta>> addedFlakes;

    /**
     * A redundant map to store a mapping from pid to a list of all flakes
     * added across containers. DO THE SAME FOR REMOVED FLAKES TOO.
     */
    private final Map<String, List<FlakeInstanceDelta>> pidToNewlyFlakeMap;

    /**
     * Map from container id to updated flakes
     * (added or removed pellet instance).
     */
    private final Map<String,
            Map<String, FlakeInstanceDelta>> updatedFlakes;

    /**
     * Map from container id to removed flakes
     * (and all pellets within that flake).
     */
    private final Map<String,
            Map<String, FlakeInstanceDelta>> removedFlakes;

    /**
     * constructor.
     * @param app Application to which this Resource mapping belongs.
     */
    public ResourceMappingDelta(final TFloeApp app) {
        this.addedFlakes = new HashMap<>();
        this.updatedFlakes = new HashMap<>();
        this.removedFlakes = new HashMap<>();
        this.pidToNewlyFlakeMap = new HashMap<>();
        this.modifiedContainers = new ArrayList<>();
        this.floeApp = app;
    }

    /**
     * resets the delta back to empty so that next iteration of changes may
     * be applied.
     */
    public final void reset() {
        this.addedFlakes.clear();
        this.updatedFlakes.clear();
        this.removedFlakes.clear();
        this.pidToNewlyFlakeMap.clear();
        this.modifiedContainers.clear();
    }

    /**
     * called whenever a new flake is added to the resource mapping.
     * @param fl the flake instance that was added
     */
    public final void flakeAdded(final ResourceMapping.FlakeInstance fl) {
        Map<String, FlakeInstanceDelta> flMap = null;
        String containerId = fl.getContainerId();

        if (!modifiedContainers.contains(containerId)) {
            modifiedContainers.add(containerId);
        }

        if (addedFlakes.containsKey(containerId)) {
            flMap = addedFlakes.get(containerId);
        } else {
            flMap = new HashMap<String, FlakeInstanceDelta>();
            addedFlakes.put(containerId, flMap);
        }

        if (!flMap.containsKey(fl.getCorrespondingPelletId())) {
            FlakeInstanceDelta fldelta = new FlakeInstanceDelta(fl);
            flMap.put(fl.getCorrespondingPelletId(), fldelta);

            List<FlakeInstanceDelta> flList = null;
            if (pidToNewlyFlakeMap.containsKey(fl.getCorrespondingPelletId())) {
                flList = pidToNewlyFlakeMap.get(
                        fl.getCorrespondingPelletId()
                );
            } else {
                flList = new ArrayList<>();
                pidToNewlyFlakeMap.put(
                        fl.getCorrespondingPelletId(),
                        flList
                );
            }
            flList.add(fldelta);
        }
    }

    /**
     * called when a flake is updated by the resource manager.
     * @param fl the flake instance that was updated.
     * @param type the update type (i.e. instance added or removed).
     */
    public final void flakeUpdated(final ResourceMapping.FlakeInstance fl,
                                   final UpdateType type) {

        Map<String, FlakeInstanceDelta> flMap = null;
        String containerId = fl.getContainerId();

        if (!modifiedContainers.contains(containerId)) {
            modifiedContainers.add(containerId);
        }

        //check if the flake was newly added/or already removed or udpdated?
        if (addedFlakes.containsKey(containerId)) {
            flMap = addedFlakes.get(containerId);
        } else if (updatedFlakes.containsKey(containerId)) {
            flMap = updatedFlakes.get(containerId);
        } else if (removedFlakes.containsKey(containerId)) {
            flMap = removedFlakes.get(containerId);
        }


        if (flMap == null) {
            flMap = new HashMap<String, FlakeInstanceDelta>();
            updatedFlakes.put(containerId, flMap);
        }

        if (!flMap.containsKey(fl.getCorrespondingPelletId())) {
            flMap.put(fl.getCorrespondingPelletId(),
                    new FlakeInstanceDelta(fl));
        }

        FlakeInstanceDelta flDelta = flMap.get(fl.getCorrespondingPelletId());

        if (type == UpdateType.InstanceAdded) {
            flDelta.instanceAdded();
        } else if (type == UpdateType.InstanceRemoved) {
            flDelta.instanceRemoved();
        } else if (type == UpdateType.ActiveAlternateChanged) {
            flDelta.activeAlternateChanged();
        }
    }

    /**
     * Flake removed from resource mapping.
     * @param fl the flake instance that was removed.
     */
    public final void flakeRemoved(final ResourceMapping.FlakeInstance fl) {
        Map<String, FlakeInstanceDelta> flMap = null;
        String containerId = fl.getContainerId();

        if (!modifiedContainers.contains(containerId)) {
            modifiedContainers.add(containerId);
        }

        if (removedFlakes.containsKey(containerId)) {
            flMap = removedFlakes.get(containerId);
        } else {
            flMap = new HashMap<String, FlakeInstanceDelta>();
            removedFlakes.put(containerId, flMap);
        }

        if (!flMap.containsKey(fl.getCorrespondingPelletId())) {
            flMap.put(fl.getCorrespondingPelletId(),
                    new FlakeInstanceDelta(fl));
        }
    }

    /**
     * @return string representation of the resource mapping delta.
     */
    @Override
    public final String toString() {
        StringBuilder builder = new StringBuilder();
        builder.append("\nNewly added Flakes: {");
        for (Map.Entry<String, Map<String, FlakeInstanceDelta>> added
                : addedFlakes.entrySet()) {
            String containerId = added.getKey();
            builder.append(containerId + "=>[");
            for (Map.Entry<String, FlakeInstanceDelta> fld
                    : added.getValue().entrySet()) {
                builder.append(fld.getKey() + ":" + fld.getValue()
                        .getNumInstancesAdded() + ", "
                        + fld.getValue().getNumInstancesRemoved() + "; ");
            }
            builder.append("]");
        }

        builder.append("}\nUpdated Flakes: {");
        for (Map.Entry<String, Map<String, FlakeInstanceDelta>> added
                : updatedFlakes.entrySet()) {
            String containerId = added.getKey();
            builder.append(containerId + "=>[");
            for (Map.Entry<String, FlakeInstanceDelta> fld
                    : added.getValue().entrySet()) {
                builder.append(fld.getKey() + ":" + fld.getValue()
                        .getNumInstancesAdded() + ", "
                        + fld.getValue().getNumInstancesRemoved() + "; ");
            }
            builder.append("]");
        }

        builder.append("}\nRemoved Flakes: {");
        for (Map.Entry<String, Map<String, FlakeInstanceDelta>> added
                : removedFlakes.entrySet()) {
            String containerId = added.getKey();
            builder.append(containerId + "=>[");
            for (Map.Entry<String, FlakeInstanceDelta> fld
                    : added.getValue().entrySet()) {
                builder.append(fld.getKey() + ":" + fld.getValue()
                        .getNumInstancesAdded() + ", "
                        + fld.getValue().getNumInstancesRemoved() + "; ");
            }
            builder.append("]");
        }

        builder.append("}");
        return builder.toString();
    }

    /**
     * @param containerId container id.
     * @return true if the container has been updated.
     */
    public final boolean isContainerUpdated(final String containerId) {
        if (addedFlakes.containsKey(containerId)) {
            return true;
        } else if (updatedFlakes.containsKey(containerId)) {
            return true;
        } else if (removedFlakes.containsKey(containerId)) {
            return true;
        }
        return false;
    }

    /**
     * @param containerId container id.
     * @return only the newly added flakes (and any pellet instances add to
     * that flake).
     */
    public final Map<String, FlakeInstanceDelta> getNewlyAddedFlakes(
            final String containerId) {
        if (addedFlakes.containsKey(containerId)) {
            return addedFlakes.get(containerId);
        }
        return null;
    }

    /**
     * @param containerId container id.
     * @return the flakes that have been updated (and any pellet instances
     * add/removed to that flake).
     */
    public final Map<String, FlakeInstanceDelta> getUpdatedFlakes(
            final String containerId) {
        if (updatedFlakes.containsKey(containerId)) {
            return updatedFlakes.get(containerId);
        }
        return null;
    }

    /**
     * @param containerId container id.
     * @return the flakes that have been removed
     */
    public final Map<String, FlakeInstanceDelta> getRemovedFlakes(
            final String containerId) {
        if (removedFlakes.containsKey(containerId)) {
            return removedFlakes.get(containerId);
        }
        return null;
    }

    /**
     * Gets the list of ONLY NEWLY ADDED preceding flakes to which the given
     * flake should connect to.
     * @param pid the pellet id.
     * @return list of preceding flakes to which the given flake should connect.
     */
    public final List<FlakeInstanceDelta> getNewlyAddedPrecedingFlakes(
            final String pid) {

        List<FlakeInstanceDelta> precedingFlakes = new ArrayList<>();

        TPellet tPellet = floeApp.get_pellets().get(pid);

        for (TEdge edge: tPellet.get_incomingEdges()) {
            String srcPid = edge.get_srcPelletId();
            if (pidToNewlyFlakeMap != null
                    && pidToNewlyFlakeMap.containsKey(srcPid)) {
                precedingFlakes.addAll(pidToNewlyFlakeMap.get(srcPid));
            }
        }
        return precedingFlakes;
    }

    /**
     * @return the number of containers that have been updated.
     */
    public final int getContainersToUpdate() {
        return modifiedContainers.size();
    }

    /**
     * class to represent the flake instance delta since the last reset.
     */
    public class FlakeInstanceDelta implements Serializable {

        /**
         * number of instances added.
         */
        private int numInstancesAdded;

        /**
         * number of instances removed.
         */
        private int numInstancesRemoved;


        /**
         * number of instances removed.
         */
        private boolean activeAlternateChanged;


        /**
         * flake instance.
         */
        private final ResourceMapping.FlakeInstance flakeInstance;


        /**
         * Constructor.
         * @param instance Flake instance.
         */
        public FlakeInstanceDelta(
                final ResourceMapping.FlakeInstance instance) {
            this.flakeInstance = instance;
            this.numInstancesAdded = 0;
            this.numInstancesRemoved = 0;
            this.activeAlternateChanged = false;
        }

        /**
         * called when an instance is added.
         */
        public final void instanceAdded() {
            this.numInstancesAdded++;
        }

        /**
         * called when an instance is removed.
         */
        public final void instanceRemoved() {
            this.numInstancesRemoved++;
        }

        /**
         * flag this flake as the active alternate has changed.
         */
        public final void activeAlternateChanged() {
            this.activeAlternateChanged = true;
        }


        /**
         * @return number of instances added to this flake.
         */
        public final int getNumInstancesAdded() {
            return this.numInstancesAdded;
        }

        /**
         * @return number of instances removed to this flake.
         */
        public final int getNumInstancesRemoved() {
            return this.numInstancesRemoved;
        }


        /**
         * @return true if the alternate for this flake has changed. Use the
         * TFloeApp to get the actual active alternate.
         */
        public final boolean isAlternateChanged() {
            return activeAlternateChanged;
        }

        /**
         * @return the corresponding flake instance.
         */
        public final ResourceMapping.FlakeInstance getFlakeInstance() {
            return flakeInstance;
        }
    }

    /**
     * Enum to describe the type of flake update.
     */
    public enum UpdateType {
        /**
         * Instance added update type.
         */
        InstanceAdded,
        /**
         * Instance removed update type.
         */
        InstanceRemoved,
        /**
         * Active Alternate changed update type.
         */
        ActiveAlternateChanged
    }
}
