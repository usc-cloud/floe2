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

package edu.usc.pgroup.floe.flake;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.ParseException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The flake process.
 *
 * @author kumbhare
 */
public final class FlakeService {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FlakeService.class);


    /**
     * Flake instance (not singleton because if we run all flakes in single
     * procress, that will not work. This way it works for both single proc
     * or multi-proc model).
     */
    private final Flake flake;

    /**
     * constructor.
     * @param fid flake's id. (the container decides a unique id for the
     *                flake)
     * @param cid container's id. This will be appended by fid to get the
     *            actual globally unique flake id. This is to support
     *            psuedo-distributed mode with multiple containers. Bug#1.
     * @param appName application's name to which this flake belongs.
     * @param jar the application's jar file name.
     * @param listeningPorts the list of ports on which this flake should
     *                       listen on. Note: This is fine here (and not as a
     *                       control signal) because this depends only on
     *                       static application configuration and not on
     */
    private FlakeService(final String fid,
                         final String cid,
                         final String appName,
                         final String jar,
                         final int[] listeningPorts) {
        flake = new Flake(fid,
                cid,
                appName,
                jar,
                listeningPorts);
    }

    /**
     * Start the flake service.
     */
    private void start() {
        flake.start();
    }


    /**
     * Entry point for the flake.
     *
     * @param args commandline arguments. (TODO)
     */
    public static void main(final String[] args) {


        Options options = new Options();

        Option idOption = OptionBuilder.withArgName("flakeId")
                                 .hasArg().isRequired()
                                 .withDescription("Container Local Flake id")
                                 .create("id");

        Option cidOption = OptionBuilder.withArgName("containerId")
                .hasArg().isRequired()
                .withDescription("Container id on which this flake resides")
                .create("cid");

        Option appNameOption = OptionBuilder.withArgName("name")
                .hasArg().isRequired()
                .withDescription("App's name to which this flake belong")
                .create("appname");

        Option jarOption = OptionBuilder.withArgName("file")
                .hasArg().withType(new String())
                .withDescription("App's jar file name containing the pellets")
                .create("jar");


        Option portsOption = OptionBuilder.withArgName("portlist")
                .hasArgs().isRequired()
                .withValueSeparator(',')
                .withDescription("App's jar file name containing the pellets")
                .create("ports");

        options.addOption(idOption);
        options.addOption(cidOption);
        options.addOption(appNameOption);
        options.addOption(jarOption);
        options.addOption(portsOption);


        CommandLineParser parser = new BasicParser();
        CommandLine line;
        try {
            line = parser.parse(options, args);

        } catch (ParseException e) {
            LOGGER.error("Invalid command: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("FlakeService", options);
            return;
        }

        String id = line.getOptionValue("id");
        String cid = line.getOptionValue("cid");
        String appName = line.getOptionValue("appname");
        String jar = null;
        if (line.hasOption("jar")) {
            jar = line.getOptionValue("jar");
        }
        String[] sports = line.getOptionValues("ports");

        LOGGER.info("id: {}", id);
        LOGGER.info("cid: {}", cid);
        LOGGER.info("app: {}", appName);
        LOGGER.info("jar: {}", jar);
        LOGGER.info("ports: {}", sports);

        int[] ports = new int[sports.length];
        try {
            for (int i = 0; i < sports.length; i++) {
                ports[i] = Integer.parseInt(sports[i]);
            }
            new FlakeService(id,
                    cid,
                    appName,
                    jar,
                    ports).start();
        } catch (Exception e) {
            LOGGER.error("Invalid port number: Exception: {}", e);
            return;
        }

        /*LOGGER.info(FloeConfig.getConfig().getString(
                ConfigProperties.SYS_JAVA_LIB_PATH));

        LOGGER.info(FloeConfig.getConfig().getString(
                ConfigProperties.SYS_JAVA_CLASS_PATH));

        //Testing ZMQ
        LOGGER.info(String.format("Version string: {}, Version int: {}",
                ZMQ.getVersionString(),
                ZMQ.getFullVersion()));*/



        //TESTING..
        /*String fileSeperator = FloeConfig.getConfig().getString(
                ConfigProperties.SYS_FILE_SEPARATOR);

        String flakesDir = FloeConfig.getConfig().getString(
                ConfigProperties.FLOE_EXEC_SCRATCH_FOLDER)
                + fileSeperator
                + FloeConfig.getConfig().getString(
                    ConfigProperties.CONTAINER_LOCAL_FOLDER)
                + fileSeperator
                + FloeConfig.getConfig().getString(
                ConfigProperties.FLAKE_LOCAL_FOLDER)
                + fileSeperator
                + "f1";


        Path flakesPath = Paths.get(flakesDir);
        LOGGER.error(flakesPath.toAbsolutePath().toString());

        DirectoryCache dirCache = new DirectoryCache(flakesPath, true);

        dirCache.addListener(new DirectoryUpdateListener() {
            @Override
            public void childrenListInitialized(
                    final Collection<FileInfo> initialChildren) {
                LOGGER.error("Initial List: {}", initialChildren);
            }

            @Override
            public void childAdded(final FileInfo addedChild) {
                LOGGER.error("Child Added {}", addedChild.getFileName());
            }

            @Override
            public void childRemoved(final FileInfo removedChild) {
                LOGGER.error("Child Removed {}", removedChild.getFileName());
            }

            @Override
            public void childUpdated(final FileInfo updatedChild) {
                LOGGER.error("Child Modified {}",
                        updatedChild.getFileName());
            }
        });

        try {
            dirCache.start();
        } catch (IOException e) {
            e.printStackTrace();
        }*/


    }
}
