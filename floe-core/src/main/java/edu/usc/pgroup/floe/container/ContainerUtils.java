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

import edu.usc.pgroup.floe.flake.FlakeService;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

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
     *
     * @param appName application name.
     * @param applicationJarPath application's jar file name.
     * @param cid container's id on which this flake resides.
     * @param listeningPorts array of ports to listen on for connections
     *                          from the succeeding pellets.
     * @return the flake id of the launched flake.
     */
    public static synchronized String launchFlake(
            final String appName, final String applicationJarPath,
            final String cid,
            final Integer[] listeningPorts) {

        final String fid  = String.valueOf(getUniqueFlakeId());


        List<String> args = new ArrayList<>();

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

        for (int i = 0; i < listeningPorts.length; i++) {
            args.add(listeningPorts[i].toString());
        }

        final String[] argsarr = new String[args.size()];
        args.toArray(argsarr);

        LOGGER.info("args: {}", argsarr);


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
     * Checks if a flake holding the instances with given appid,pelletid
     * already exists.
     * @param appId application id
     * @param pelletId pellet id
     */
    public static void isFlakeExist(final String appId,
                                    final String pelletId) {

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
     */
    public static void sendConnectCommand(final String fid, final String host,
                                          final int assignedPort) {

        String connectionString
                = Utils.Constants.FLAKE_RECEIVER_FRONTEND_CONNECT_SOCK_PREFIX
                + host + ":"
                + assignedPort;
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
}
