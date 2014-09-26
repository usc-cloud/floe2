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

import edu.usc.pgroup.floe.flake.FlakeInfo;
import edu.usc.pgroup.floe.flake.FlakeService;
import edu.usc.pgroup.floe.resourcemanager.ResourceMapping;
import edu.usc.pgroup.floe.resourcemanager.ResourceMappingDelta;
import edu.usc.pgroup.floe.thriftgen.TPellet;
import edu.usc.pgroup.floe.utils.RetryLoop;
import edu.usc.pgroup.floe.utils.RetryPolicyFactory;
import edu.usc.pgroup.floe.utils.Utils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * @author kumbhare
 */
public final class ContainerUtils {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ContainerUtils.class);

    /**
     * Flake Id.
     */
    private static int flakeId = 0;


    /**
     * Launches a new Flake instance.
     * Currently its a in proc. Think about if we need to do this in a new
     * process?
     * @param pid pellet id/name.
     * @param appName application name.
     * @param applicationJarPath application's jar file name.
     * @param cid container's id on which this flake resides.
     * @param pelletPortMap map of pellet name to ports to listen on for
     *                      connections from the succeeding pellets.
     * @param backChannelPortMap map of port for the dispersion. One port
     *                           per target pellet.
     * @param channelTypeMap Map of target pellet to channel type (one per edge)
     * @param pelletStreamsMap map of pellet name to list of stream names.
     * @return the flake id of the launched flake.
     */
    public static synchronized String launchFlake(
            final String pid,
            final String appName,
            final String applicationJarPath,
            final String cid,
            final Map<String, Integer> pelletPortMap,
            final Map<String, Integer> backChannelPortMap,
            final Map<String, String> channelTypeMap,
            final Map<String, List<String>> pelletStreamsMap) {

        final String fid  = String.valueOf(getUniqueFlakeId());


        List<String> args = new ArrayList<>();

        args.add("-pid");
        args.add(pid);
        args.add("-id");
        args.add(fid);
        args.add("-appname");
        args.add(appName);
        if (applicationJarPath != null) {
            args.add("-jar");
            args.add(applicationJarPath);
        }
        args.add("-cid");
        args.add(cid);
        args.add("-ports");
        for (Map.Entry<String, Integer> p: pelletPortMap.entrySet()) {
            args.add(p.getKey() + ':' + p.getValue());
        }

        args.add("-backchannelports");
        for (Map.Entry<String, Integer> p: backChannelPortMap.entrySet()) {
            args.add(p.getKey() + ':' + p.getValue());
        }

        args.add("-channeltype");
        for (Map.Entry<String, String> p: channelTypeMap.entrySet()) {
            args.add(p.getKey() + ':' + p.getValue());
        }

        args.add("-streams");
        for (Map.Entry<String, List<String>> streams
                : pelletStreamsMap.entrySet()) {
            args.add(streams.getKey() + ':'
                    + StringUtils.join(streams.getValue(), "|"));
        }

        final String[] argsarr = new String[args.size()];
        args.toArray(argsarr);

        LOGGER.info("args: {}", args);


        Thread t = new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        FlakeService.main(
                                argsarr
                        );
                    }
                }
        );
        t.start();
        return Utils.generateFlakeId(cid, fid);
    }

    /**
     * Returns the unique flake id.
     * @return container-local unique flake id
     */
    public static int getUniqueFlakeId() {
        return flakeId++;
    }

    /**
     * hiding default constructor.
     */
    private ContainerUtils() {

    }

    /**
     * Sends the connect command to the given flake (using ipc).
     * @param fid flake's id to which to send the command.
     * @param host the host to connect to.
     * @param assignedPort the port to connect to.
     * @param backPort the port for the back channel to connect to.
     */
    public static void sendConnectCommand(final String fid, final String host,
                                          final int assignedPort,
                                          final int backPort) {

        String dataConnectStr
                = Utils.Constants.FLAKE_RECEIVER_FRONTEND_CONNECT_SOCK_PREFIX
                + host + ":"
                + assignedPort;

        String backChannelConnectStr
                = Utils.Constants.FLAKE_RECEIVER_FRONTEND_CONNECT_SOCK_PREFIX
                + host + ":"
                + backPort;

        String connectionString = dataConnectStr + ";" + backChannelConnectStr;

        FlakeControlCommand command = new FlakeControlCommand(
                FlakeControlCommand.Command.CONNECT_PRED, connectionString);
        FlakeControlSignalSender.getInstance().send(fid, command);
    }

    /**
     * sends the increment pellet command to the given flake. (using ipc)
     * @param fid flake's id to which to send the command.
     * @param serializedPellet serialized pellet received from the user.
     */
    public static void sendIncrementPelletCommand(final String fid,
                                                  final byte[]
                                                          serializedPellet) {
        FlakeControlCommand command = new FlakeControlCommand(
                FlakeControlCommand.Command.INCREMENT_PELLET,
                serializedPellet
        );
        FlakeControlSignalSender.getInstance().send(fid, command);
    }


    /**
     * sends the decrement pellet command to the given flake. (using ipc)
     * @param fid flake id to terminate a pellet instance.
     */
    private static void sendDecrementPelletCommand(final String fid) {
        FlakeControlCommand command = new FlakeControlCommand(
                FlakeControlCommand.Command.DECREMENT_PELLET,
                null
        );
        FlakeControlSignalSender.getInstance().send(fid, command);
    }

    /**
     * sends the decrement pellet command to the given flake. (using ipc)
     * @param fid flake id to terminate a pellet instance.
     */
    public static void sendStopAppPelletsCommand(final String fid) {
        FlakeControlCommand command = new FlakeControlCommand(
                FlakeControlCommand.Command.DECREMENT_ALL_PELLETS,
                null
        );
        FlakeControlSignalSender.getInstance().send(fid, command);
    }

    /**
     * Send the start pellets command to the given flake.
     * @param fid flake id to terminate a pellet instance.
     */
    public static void sendStartPelletsCommand(final String fid) {
        FlakeControlCommand command = new FlakeControlCommand(
                FlakeControlCommand.Command.START_PELLETS,
                null
        );
        FlakeControlSignalSender.getInstance().send(fid, command);
    }

    /**
     * sends a switch alternate command to the given flake with the new
     * alternate's implementation sent as data. NOTE: Not send the jar since
     * for current version we assume all implementations are available in the
     * initially submitted jar file.
     * @param fid flake id to switch the alternate.
     * @param activeAlternate new alternate implementation.
     */
    private static void sendSwitchAlternateCommand(
            final String fid,
            final byte[] activeAlternate) {
        FlakeControlCommand command = new FlakeControlCommand(
                FlakeControlCommand.Command.SWITCH_ALTERNATE,
                activeAlternate
        );
        FlakeControlSignalSender.getInstance().send(fid, command);
    }

    /**
     * sends a kill self command to the flake.
     * @param fid flake id which has to be killed.
     */
    public static void sendKillFlakeCommand(final String fid) {
        FlakeControlCommand command = new FlakeControlCommand(
                FlakeControlCommand.Command.TERMINATE,
                null
        );
        FlakeControlSignalSender.getInstance().send(fid, command);
    }

    /**
     * Launches flakes and initializes pellets based on the list of flakes
     * given.
     * @param resourceMapping current resource mapping as determined by the
     *                        resource manager.
     * @param containerId container id.
     * @param flakes map of pellet id/name to flake instances from the
     *               resource mapping allocated to this container.
     */
    public static void launchFlakesAndInitializePellets(
            final ResourceMapping resourceMapping,
            final String containerId,
            final Map<String, ResourceMapping.FlakeInstance> flakes) {

        //we can go ahead and create the flakes since this is a newly
        // added application.
        if (flakes != null && flakes.size() > 0) {
            //Step 1. Launch Flakes.
            Map<String, String> pidToFidMap = createFlakes(
                    resourceMapping.getAppName(),
                    resourceMapping.getApplicationJarPath(),
                    containerId,
                    flakes);

            if (pidToFidMap == null) {
                //TODO: write status to zookeeper.
                LOGGER.error("Could not launch the appropriate flakes "
                        + "suggested by the resource manager.");
                return;
            }

            //Step 2, previously Step 3. Launch pellets.
            LOGGER.info("Launching pellets.");
            for (Map.Entry<String, ResourceMapping.FlakeInstance> flakeEntry
                    : flakes.entrySet()) {
                String pid = flakeEntry.getKey();

                ResourceMapping.FlakeInstance flakeInstance
                        = flakeEntry.getValue();

                TPellet pellet = resourceMapping.getFloeApp().get_pellets()
                        .get(flakeInstance.getCorrespondingPelletId());

                byte[] activeAlternate = pellet.get_alternates().get(
                        pellet.get_activeAlternate()
                ).get_serializedPellet();

                LOGGER.info("Creating {} instances.",
                        flakeInstance.getNumPelletInstances());
                for (int i = 0;
                     i < flakeInstance.getNumPelletInstances();
                     i++) {
                    ContainerUtils.sendIncrementPelletCommand(
                            pidToFidMap.get(pid),
                            activeAlternate
                    );
                }

            }

            //Step 3, previously Step 2. Send connect signals to the flakes.
            //It has to connect to all the preceding signals.
            LOGGER.info("Sending connect signals.");
            for (Map.Entry<String, ResourceMapping.FlakeInstance> flakeEntry
                    : flakes.entrySet()) {

                String pid = flakeEntry.getKey();
                List<ResourceMapping.FlakeInstance> preds
                        = resourceMapping
                        .getPrecedingFlakes(pid);

                for (ResourceMapping.FlakeInstance pred: preds) {
                    int assignedPort = pred.getAssignedPort(pid);
                    int backPort = pred.getAssignedBackPort(pid);
                    String host = pred.getHost();
                    ContainerUtils.sendConnectCommand(
                            pidToFidMap.get(pid),
                            host, assignedPort, backPort);
                }
            }
        }
    }

    /**
     * updates the flakes by adding or removing the pellet instances based on
     * the flake deltas.
     * @param resourceMapping current resource mapping as determined by the
     *                        resource manager.
     * @param flakeDeltas the updates to the flakes containing information
     *                    about flakes that have to be updated by adding or
     *                    removing the pellet instances.
     */
    public static void updateFlakes(
            final ResourceMapping resourceMapping,
            final Map<String,
                    ResourceMappingDelta.FlakeInstanceDelta> flakeDeltas) {

        if (flakeDeltas != null && flakeDeltas.size() > 0) {
            for (Map.Entry<String,
                    ResourceMappingDelta.FlakeInstanceDelta> entry
                    : flakeDeltas.entrySet()) {


                final String pelletId = entry.getKey();
                ResourceMappingDelta.FlakeInstanceDelta delta
                        = entry.getValue();

                TPellet pellet = resourceMapping.getFloeApp().get_pellets()
                        .get(delta.getFlakeInstance()
                                .getCorrespondingPelletId());

                byte[] activeAlternate = pellet.get_alternates().get(
                        pellet.get_activeAlternate()
                ).get_serializedPellet();

                try {
                    FlakeInfo info = RetryLoop.callWithRetry(RetryPolicyFactory
                                    .getDefaultPolicy(),
                            new Callable<FlakeInfo>() {
                                @Override
                                public FlakeInfo call() throws Exception {
                                    return FlakeMonitor.getInstance()
                                            .getFlakeInfo(pelletId);
                                }
                            });
                    LOGGER.info("Found Flake:{}", info.getFlakeId());


                    //update flake here.

                    int diffPellets = delta.getNumInstancesAdded()
                            - delta.getNumInstancesRemoved();


                    if (diffPellets > 0) {
                        //TO INCREMENT.
                        LOGGER.info("Incrementing pellet instances for flake: "
                                + "{} by {}", info.getFlakeId(), diffPellets);
                        for (int i = 0;
                             i < diffPellets;
                             i++) {
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
                        for (int i = 0;
                             i < diffPellets;
                             i++) {
                            ContainerUtils.sendDecrementPelletCommand(
                                    info.getFlakeId()
                            );
                        }
                    }

                    if (delta.isAlternateChanged()) {
                        LOGGER.info("Alternate Changed for :{}",
                                info.getFlakeId());

                        ContainerUtils.sendSwitchAlternateCommand(
                                info.getFlakeId(),
                                activeAlternate
                        );
                    }

                } catch (Exception e) {
                    LOGGER.error("Could not start flake");
                    return;
                }
            }
        }

    }

    /**
     * Function to launch flakes within the container. This will launch
     * flakes and wait for a heartbeat from each of them.
     *
     * @param appName application name.
     * @param applicationJarPath application's jar file name.
     * @param containerId Container id.
     * @param flakes list of flake instances from the resource mapping.
     * @return the pid to fid map.
     */
    public static Map<String, String> createFlakes(
            final String appName, final String applicationJarPath,
            final String containerId,
            final Map<String, ResourceMapping.FlakeInstance> flakes) {

        Map<String, String> pidToFidMap = new HashMap<>();



        for (Map.Entry<String, ResourceMapping.FlakeInstance> entry
                : flakes.entrySet()) {
            final String pid = entry.getKey();

            ResourceMapping.FlakeInstance flakeInstance = entry.getValue();
            final String fid = ContainerUtils.launchFlake(
                    pid,
                    appName,
                    applicationJarPath,
                    containerId,
                    flakeInstance.getPelletPortMapping(),
                    flakeInstance.getPelletBackChannelPortMapping(),
                    flakeInstance.getPelletChannelTypeMapping(),
                    flakeInstance.getPelletStreamsMapping());

            try {
                FlakeInfo info = RetryLoop.callWithRetry(RetryPolicyFactory
                                .getDefaultPolicy(),
                        new Callable<FlakeInfo>() {
                            @Override
                            public FlakeInfo call() throws Exception {
                                return FlakeMonitor.getInstance()
                                        .getFlakeInfo(pid);
                            }
                        });
                LOGGER.info("Flake started (at container):{}",
                        info.getFlakeId());
                pidToFidMap.put(entry.getKey(), info.getFlakeId());
            } catch (Exception e) {
                LOGGER.error("Could not start flake");
                return null;
            }
        }
        return pidToFidMap;
    }

    /**
     * Removes the flakes from the container.
     * @param resourceMapping current resource mapping as determined by the
     *                        resource manager.
     * @param flakeDeltas the updates to the flakes containing information
     *                    about flakes that have to be removed.
     */
    public static void terminateFlakes(
            final ResourceMapping resourceMapping,
            final Map<String,
                    ResourceMappingDelta.FlakeInstanceDelta> flakeDeltas
    ) {
        if (flakeDeltas != null && flakeDeltas.size() > 0) {
            for (Map.Entry<String,
                    ResourceMappingDelta.FlakeInstanceDelta> entry
                    : flakeDeltas.entrySet()) {
                final String pelletId = entry.getKey();
                ResourceMappingDelta.FlakeInstanceDelta delta
                        = entry.getValue();

                try {
                    FlakeInfo info = RetryLoop.callWithRetry(RetryPolicyFactory
                                    .getDefaultPolicy(),
                            new Callable<FlakeInfo>() {
                                @Override
                                public FlakeInfo call() throws Exception {
                                    return FlakeMonitor.getInstance()
                                            .getFlakeInfo(pelletId);
                                }
                            });
                    LOGGER.info("Found Flake:{}. Sending terminate signal",
                            info.getFlakeId());
                    ContainerUtils.sendKillFlakeCommand(info.getFlakeId());
                } catch (Exception e) {
                    LOGGER.error("Could not kill flake.");
                    return;
                }
            }
        }
    }
}
