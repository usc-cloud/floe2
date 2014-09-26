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

import edu.usc.pgroup.floe.coordinator.transitions.ClusterTransition;
import edu.usc.pgroup.floe.resourcemanager.ResourceMapping;
import edu.usc.pgroup.floe.signals.ContainerSignal;
import edu.usc.pgroup.floe.signals.SignalHandler;
import edu.usc.pgroup.floe.thriftgen.AppNotFoundException;
import edu.usc.pgroup.floe.thriftgen.AppStatus;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.apache.curator.framework.recipes.barriers.DistributedDoubleBarrier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.UUID;

/**
 * @author kumbhare
 */
public class KillAppTransition extends ClusterTransition {
    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(KillAppTransition.class);

    /**
     * Start the execution for the given transition. Returns immediately
     * after starting?
     *
     * @param args transaction specific arguments
     * @throws Exception if there is an unrecoverable error while
     *                   processing the transition.
     */
    @Override
    protected final void execute(final Map<String, Object> args) throws
            Exception {
        LOGGER.info("Executing StartApp transition.");

        String appName = (String) args.get("appName");

        String appUpdateBarrierPath = ZKUtils
                .getApplicationBarrierPath(appName);

        String resourceMappingPath = ZKUtils
                .getApplicationResourceMapPath(appName);

        byte[] serializedRM = null;

        //Step 1. Verify that the app exists.
        if (!ZKUtils.appExists(appName)) {
            LOGGER.error("Application does not exist.");
            throw new AppNotFoundException();
        }

        serializedRM = ZKClient.getInstance().getCuratorClient().getData()
                .forPath(resourceMappingPath);

        ResourceMapping resourceMapping =
                (ResourceMapping) Utils.deserialize(serializedRM);

        int numContainersToUpdate = resourceMapping.getContainersToUpdate();

        DistributedDoubleBarrier barrier = new DistributedDoubleBarrier(
                ZKClient.getInstance().getCuratorClient(),
                appUpdateBarrierPath,
                numContainersToUpdate + 1
        );

        //Step 2. Kill Pellets.
        SignalHandler.getInstance().signal(appName, "ALL-CONTAINERS",
                ContainerSignal.ContainerSignalType.STOP_PELLETS,
                Utils.serialize("dummy"));

        ZKUtils.setAppStatus(appName,
                AppStatus.UPDATING_PELLETS);

        barrier.enter();
        LOGGER.info("Waiting for containers to stop all pellets.");
        barrier.leave();
        ZKUtils.setAppStatus(appName,
                AppStatus.UPDATING_PELLETS_COMPLETED);

        LOGGER.info("All pellets stopped.");

        //Step 3. Terminate flakes.
        SignalHandler.getInstance().signal(appName, "ALL-CONTAINERS",
                ContainerSignal.ContainerSignalType.TERMINATE_FLAKES,
                Utils.serialize("dummy"));

        ZKUtils.setAppStatus(appName,
                AppStatus.UPDATING_FLAKES);

        barrier.enter();
        LOGGER.info("Waiting for containers to terminate flakes.");
        barrier.leave();
        ZKUtils.setAppStatus(appName,
                AppStatus.UPDATING_FLAKES_COMPLETED);

        LOGGER.info("All flakes terminated.");

        //Step 4. Move the app from running to terminated.
        String terminatedAppPath = ZKUtils
                .getApplicationTerminatedInfoPath(appName + "-"
                        + UUID.randomUUID());
        //Copy data to terminated section. Currently it is just the RM but
        // later we will have much more.

        ZKClient.getInstance().getCuratorClient().create()
                .creatingParentsIfNeeded()
                .forPath(terminatedAppPath, serializedRM);

        ZKUtils.setAppStatus(appName, AppStatus.TERMINATED);
        LOGGER.info("App terminated. Moving to archive.");

        //Now delete from the aaps path.
        String appPath = ZKUtils.getApplicationPath(appName);
        ZKClient.getInstance().getCuratorClient().delete()
                .deletingChildrenIfNeeded().forPath(appPath);

        LOGGER.info("Moved to archive.");
    }

    /**
     * @return gets the name of the transaction.
     */
    @Override
    public final String getName() {
        return "KillTransaction";
    }
}
