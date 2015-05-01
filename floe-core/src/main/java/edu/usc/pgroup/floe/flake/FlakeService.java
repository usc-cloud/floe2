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
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

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
     * process, that will not work. This way it works for both single proc
     * or multi-proc model).
     */
    private final Flake flake;

    /**
     * constructor.
     * @param pid Unique Pellet id/name received from the user.
     * @param fid flake's id. (the container decides a unique id for the
     *                flake)
     * @param cid container's id. This will be appended by fid to get the
 *            actual globally unique flake id. This is to support
 *            psuedo-distributed mode with multiple containers. Bug#1.
     * @param appName application's name to which this flake belongs.
     * @param jar the application's jar file name.
     * @param pluginJar plugin jar.
     */
    private FlakeService(final String pid,
                         final String fid,
                         final String cid,
                         final String appName,
                         final String jar,
                         final String pluginJar) {
        flake = new Flake(pid, fid,
                cid,
                appName,
                jar,
                pluginJar);
    }

    /**
     * Start the flake service.
     */
    private void start() {
        flake.start();
        LOGGER.info("Flake execution completed. Exiting now.");
    }


    /**
     * Builds the CLI options.
     * @return the configured options required for commandline execution.
     */
    private static Options buildOptions() {
        Options options = new Options();

        Option pidOption = OptionBuilder.withArgName("Pellet id")
                .hasArg().isRequired()
                .withDescription("Pellet id/name that should run on the flake")
                .create("pid");

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

        Option pluginjarOption = OptionBuilder.withArgName("file")
                .hasArg().withType(new String())
                .withDescription("App's plugin jar")
                .create("pluginjar");


        options.addOption(pidOption);
        options.addOption(idOption);
        options.addOption(cidOption);
        options.addOption(appNameOption);
        options.addOption(jarOption);
        options.addOption(pluginjarOption);

        return options;
    }

    /**
     * Entry point for the flake.
     *
     * @param args commandline arguments. (TODO)
     */
    public static void main(final String[] args) {

        Options options = buildOptions();

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

        String pid = line.getOptionValue("pid");
        String id = line.getOptionValue("id");
        String cid = line.getOptionValue("cid");
        String appName = line.getOptionValue("appname");
        String token = line.getOptionValue("token");
        String jar = null;
        if (line.hasOption("jar")) {
            jar = line.getOptionValue("jar");
        }
        String pluginjar = null;
        if (line.hasOption("pluginjar")) {
            pluginjar = line.getOptionValue("pluginjar");
        }

        LOGGER.info("pid: {}, id:{}, cid:{}, app:{}, jar:{}",
            pid, id,
            cid,
            appName, jar);
        try {
            new FlakeService(pid,
                    id,
                    cid,
                    appName,
                    jar, pluginjar).start();
        } catch (Exception e) {
            LOGGER.error("Exception while creating flake: {}", e);
            return;
        }
    }

    /**
     * To parse the command line argument for streams.
     * @param s the commandeline streams parameter
     * @return list of items
     */
    private static List<String> parseCSV(final String s) {
        //THIS IS NOT A GOOD CODE. TRY A BETTER WAY
        LOGGER.info("Parsing {}", s);
        return Arrays.asList(s.split("\\|"));
    }
}
