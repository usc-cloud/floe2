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
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.zkcache.PathChildrenUpdateListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author kumbhare
 */
public class AppsAssignmentMonitor {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(AppsAssignmentMonitor.class);

    /**
     * ZK applications assignment cache.
     */
    private ZKAppsCache appsCache;

    /**
     * container id.
     */
    private String containerId;

    /**
     * constructor.
     * @param cid container id.
     */
    public AppsAssignmentMonitor(final String cid) {
        appsCache = new ZKAppsCache(new AppAssignmentListener(),
                                    cid);
        this.containerId = cid;
    }

    /**
     * Apps assignment listener.
     */
    public class AppAssignmentListener implements PathChildrenUpdateListener {

        /**
         * Triggered when initial list of children is cached.
         * This is retrieved synchronously.
         *
         * @param initialChildren initial list of children.
         */
        @Override
        public void childrenListInitialized(
                final Collection<ChildData> initialChildren) {
            //TODO: THIS IS FOR A CONTAINER TO RECOVER. WILL DO LATER.
        }

        /**
         * Triggered when a new child is added.
         * Note: this is not recursive.
         *
         * @param addedChild newly added child's data.
         */
        @Override
        public final void childAdded(final ChildData addedChild) {
            //New application is added.
            //Check for any assignments made to this container and start
            // flakes as required.
            //We still need to check for existing Flakes since multiple
            // pellet instances for a given pellet might be needed.
            byte[] ser = addedChild.getData();
            ResourceMapping resourceMapping =
                    (ResourceMapping) Utils.deserialize(ser);


            ResourceMapping.ContainerInstance container
                    = resourceMapping.getContainer(containerId);


            String applicationJar = resourceMapping.getApplicationJarPath();

            if (applicationJar != null) {
                try {

                    String downloadLocation = Utils.getContainerJarDownloadPath(
                            resourceMapping.getAppName(), applicationJar);

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

            LOGGER.info("Container Id: " + containerId);
            LOGGER.info("Resource Mapping {}: ", resourceMapping);


            if (container == null) {
                return;
            }

            Map<String, ResourceMapping.FlakeInstance> flakes
                    = container.getFlakes();

            ContainerUtils.launchFlakesAndInitializePellets(resourceMapping,
                    containerId,
                    flakes);
        }

        /**
         * Triggered when an existing child is removed.
         * Note: this is not recursive.
         *
         * @param removedChild removed child's data.
         */
        @Override
        public void childRemoved(final ChildData removedChild) {

        }

        /**
         * Triggered when a child is updated.
         * Note: This is called only when Children data is also cached in
         * addition to stat information.
         *
         * @param updatedChild update child's data.
         */
        @Override
        public final void childUpdated(final ChildData updatedChild) {
            //application resource mapping is updated.
            //Check for any assignments made to this container and
            // start/update/remove flakes as required.

            //We still need to check for existing Flakes since multiple
            // pellet instances for a given pellet might be needed.

            byte[] ser = updatedChild.getData();
            ResourceMapping resourceMapping =
                    (ResourceMapping) Utils.deserialize(ser);



            String applicationJar = resourceMapping.getApplicationJarPath();

            if (applicationJar != null) {
                try {

                    String downloadLocation = Utils.getContainerJarDownloadPath(
                            resourceMapping.getAppName(), applicationJar);

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

            LOGGER.info("Container Id: " + containerId);
            LOGGER.info("Application Resource Mapping updated. {}",
                    resourceMapping.getDelta());

            boolean containerUpdated =
                    resourceMapping.getDelta().isContainerUpdated(containerId);

            if (!containerUpdated) {
                //No changes found for this container.
                LOGGER.info("No updates observed for the container: {}.",
                        containerId);
                return;
            }

            //Create new flakes if any.
            Map<String, ResourceMappingDelta.FlakeInstanceDelta>
                    addedflakeDeltas
                    = resourceMapping.getDelta().getNewlyAddedFlakes(
                    containerId);

            if (addedflakeDeltas != null) {
                //Since this is a new flake, all instances are newly added.
                Map<String, ResourceMapping.FlakeInstance> flakes
                        = new HashMap<>();

                for (Map.Entry<String, ResourceMappingDelta.FlakeInstanceDelta>
                        fd : addedflakeDeltas.entrySet()) {
                    flakes.put(fd.getKey(), fd.getValue().getFlakeInstance());
                }

                if (flakes != null) {
                    ContainerUtils.launchFlakesAndInitializePellets(
                            resourceMapping, containerId, flakes);
                }
            }


            //IMP. Check if NEW FLAKES were created for any of the preceding
            // pellets. If so connect to them.

            //Get a list of flakes running on this container.
            Map<String, FlakeInfo> runningFlakes
                    = FlakeMonitor.getInstance().getFlakes();

            for (Map.Entry<String, FlakeInfo> entry
                    : runningFlakes.entrySet()) {

                //THIS IS REQUIRED ONLY IF THIS IS NOT A NEWLY CREATED PELLET
                // . BECAUSE THE launchAndInitialize function will take care
                // of connections.
                //SO CHECK IF THIS PELLET IS PRESENT IN THE NEWLY ADDED LIST.
                if (addedflakeDeltas != null
                        && addedflakeDeltas.containsKey(entry.getKey())) {
                    continue;
                }

                String pelletName = entry.getKey();
                FlakeInfo flakeInfo = entry.getValue();

                //get DELTA ONLY those predecessor flakes that have been
                // added for this pellet.
                List<ResourceMappingDelta.FlakeInstanceDelta> newPredFlakes
                        = resourceMapping.getDelta()
                            .getNewlyAddedPrecedingFlakes(pelletName);

                for (ResourceMappingDelta.FlakeInstanceDelta predDelta
                        : newPredFlakes) {
                    ResourceMapping.FlakeInstance pred
                            = predDelta.getFlakeInstance();
                    int assignedPort = pred.getAssignedPort(pelletName);
                    String host = pred.getHost();
                    ContainerUtils.sendConnectCommand(
                            flakeInfo.getFlakeId(),
                            host, assignedPort);
                }

            }

            //Update any flakes if required (i.e. increase or decrease pellet
            // instances.).
            Map<String,
                    ResourceMappingDelta.FlakeInstanceDelta> flakeDeltasUpdated
                    = resourceMapping.getDelta().getUpdatedFlakes(containerId);

            if (flakeDeltasUpdated != null) {
                ContainerUtils.updateFlakes(resourceMapping,
                        flakeDeltasUpdated);
            }

            //Remove any flakes if required.

        }


    }
}
