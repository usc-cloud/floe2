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
import edu.usc.pgroup.floe.utils.RetryLoop;
import edu.usc.pgroup.floe.utils.RetryPolicyFactory;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.zkcache.PathChildrenUpdateListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

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

            try {

                String downloadLocation = Utils.getContainerJarDownloadPath(
                    resourceMapping.getAppName(), applicationJar);

                LOGGER.info("Downloading: " + applicationJar);
                FloeClient.getInstance().downloadFileSync(applicationJar,
                        downloadLocation);
                LOGGER.info("Finished Downloading: " + applicationJar);
            } catch (TTransportException e) {
                e.printStackTrace();
            }

            LOGGER.info("Container Id: " + containerId);
            LOGGER.info("Resource Mapping {}: ", resourceMapping);


            if (container == null) {
                return;
            }

            Map<String, ResourceMapping.FlakeInstance> flakes
                    = container.getFlakes();

            //we can go ahead and create the flakes since this is a newly
            // added application.
            if (flakes != null && flakes.size() > 0) {
                //Step 1. Launch Flakes.
                Map<String, String> pidToFidMap = createFlakes(
                        resourceMapping.getAppName(),
                        resourceMapping.getApplicationJarPath(),
                        flakes);

                if (pidToFidMap == null) {
                    //TODO: write status to zookeeper.
                    LOGGER.error("Could not launch the appropriate flakes "
                            + "suggested by the resource manager.");
                }

                //Step 2. Send connect signals to the flakes.
                for (Map.Entry<String, ResourceMapping.FlakeInstance> flakeEntry
                        : flakes.entrySet()) {

                    String pid = flakeEntry.getKey();
                    List<ResourceMapping.FlakeInstance> preds
                            = resourceMapping
                            .getPrecedingFlakes(pid);

                    for (ResourceMapping.FlakeInstance pred: preds) {
                        int assignedPort = pred.getAssignedPort(pid);
                        String host = pred.getHost();
                        ContainerUtils.sendConnectCommand(
                                pidToFidMap.get(pid),
                                host, assignedPort);
                    }
                }

                //Step 3. Launch pellets.
                for (Map.Entry<String, ResourceMapping.FlakeInstance> flakeEntry
                        : flakes.entrySet()) {
                    String pid = flakeEntry.getKey();

                    ResourceMapping.FlakeInstance flakeInstance
                            = flakeEntry.getValue();

                    for (ResourceMapping.PelletInstance instance
                            : flakeInstance.getPelletInstances()) {
                        ContainerUtils.sendIncrementPelletCommand(
                                pidToFidMap.get(pid),
                                flakeInstance.getSerializedPellet()
                        );
                    }

                }
            }

            /*for (ResourceMapping.PelletInstance instance
                    :resourceMapping.getPelletInstances(containerId)) {

                //Launch flakes here.
                final String fid = ContainerUtils.launchFlake(2);

                try {
                    FlakeInfo info = RetryLoop.callWithRetry(RetryPolicyFactory
                                    .getDefaultPolicy(),
                            new Callable<FlakeInfo>() {
                                @Override
                                public FlakeInfo call() throws Exception {
                                    return FlakeMonitor.getInstance()
                                            .getFlakeInfo(fid);
                                }
                            });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }*/
        }

        /**
         * Triggered when an existing child is removed.
         * Note: this is not recursive.
         *
         * @param removedChild removed child's data.
         */
        @Override
        public void childRemoved(final ChildData removedChild) {
            //Application is removed.
        }

        /**
         * Triggered when a child is updated.
         * Note: This is called only when Children data is also cached in
         * addition to stat information.
         *
         * @param updatedChild update child's data.
         */
        @Override
        public void childUpdated(final ChildData updatedChild) {
            //Application resource mapping is updated.
        }

        /**
         * Function to launch flakes within the container. This will launch
         * flakes and wait for a heartbeat from each of them.
         *
         * @param appName application name.
         * @param applicationJarPath application's jar file name.
         * @param flakes list of flake instances from the resource mapping.
         * @return the pid to fid map.
         */
        private Map<String, String> createFlakes(
                final String appName, final String applicationJarPath,
                final Map<String, ResourceMapping.FlakeInstance> flakes) {

            Map<String, String> pidToFidMap = new HashMap<>();



            for (Map.Entry<String, ResourceMapping.FlakeInstance> entry
                    : flakes.entrySet()) {
                ResourceMapping.FlakeInstance flakeInstance = entry.getValue();
                final String fid = ContainerUtils.launchFlake(appName,
                        applicationJarPath,
                        flakeInstance.getListeningPorts());

                try {
                    FlakeInfo info = RetryLoop.callWithRetry(RetryPolicyFactory
                                    .getDefaultPolicy(),
                            new Callable<FlakeInfo>() {
                                @Override
                                public FlakeInfo call() throws Exception {
                                    return FlakeMonitor.getInstance()
                                            .getFlakeInfo(fid);
                                }
                            });

                    pidToFidMap.put(entry.getKey(), info.getFlakeId());
                } catch (Exception e) {
                    LOGGER.error("Could not start flake");
                    return null;
                }
            }
            return pidToFidMap;
        }
    }
}
