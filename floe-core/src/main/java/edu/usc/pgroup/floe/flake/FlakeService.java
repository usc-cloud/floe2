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

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
     * @param statePort       Port to be used for sending checkpoint data.
     * @param pelletPortMap the list of ports on which this flake should
*                       listen on. Note: This is fine here (and not as a
*                       control signal) because this depends only on
*                       static application configuration and not on
     * @param backChannelPortMap map of port for the dispersion. One port
*                           per target pellet.
     * @param successorChannelTypeMap Map of target pellet to channel type
*                                (one per edge)
     * @param predChannelTypeMap Map of src pellet to channel type
*                                (one per edge)
     * @param pelletStreamsMap map from successor pellets to subscribed
     * @param token token from the container.
     */
    private FlakeService(final String pid,
                         final String fid,
                         final String cid,
                         final String appName,
                         final String jar,
                         final int statePort,
                         final Map<String, Integer> pelletPortMap,
                         final Map<String, Integer> backChannelPortMap,
                         final Map<String, String> successorChannelTypeMap,
                         final Map<String, String> predChannelTypeMap,
                         final Map<String, List<String>> pelletStreamsMap,
                         final String token) {
        flake = new Flake(pid, fid,
                cid,
                appName,
                jar,
                statePort,
                pelletPortMap,
                backChannelPortMap,
                successorChannelTypeMap,
                predChannelTypeMap,
                pelletStreamsMap,
                token);
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


        Option portsOption = OptionBuilder.withArgName("pellet:port list")
                .hasArgs().isRequired()
                .withValueSeparator(',')
                .withDescription("List of ports for data channel.")
                .create("ports");

        Option backPortsOption = OptionBuilder.withArgName("pellet:port list")
                .hasArgs().isRequired()
                .withValueSeparator(',')
                .withDescription("List of ports for back channel.")
                .create("backchannelports");

        Option channelTypeOption = OptionBuilder.withArgName("pellet:channel "
                + "type")
                .withValueSeparator(',')
                .hasArgs().isRequired()
                .withDescription("Type of the channel per out edge.")
                .create("channeltype");

        Option predChannelTypeOption = OptionBuilder.withArgName(
                "pellet:channel type")
                .withValueSeparator(',')
                .hasArgs().isRequired()
                .withDescription("Type of the channel per incoming edge.")
                .create("predchanneltype");

        Option streamsOption = OptionBuilder.withArgName("pellet:<streams> "
                + "list").hasArgs().isRequired()
                .withValueSeparator(',')
                .withDescription("App's jar file name containing the pellets")
                .create("streams");

        Option statePortOption = OptionBuilder.withArgName("stateport:<num>")
                .hasArgs().isRequired()
                .withDescription("Port number to use for state checkpointing")
                .create("stateport");

        Option tokenOption = OptionBuilder.withArgName("token:<num>")
                .hasArgs().isRequired()
                .withDescription("Port number to use for state checkpointing")
                .create("token");

        options.addOption(pidOption);
        options.addOption(idOption);
        options.addOption(cidOption);
        options.addOption(appNameOption);
        options.addOption(jarOption);
        options.addOption(portsOption);
        options.addOption(backPortsOption);
        options.addOption(streamsOption);
        options.addOption(channelTypeOption);
        options.addOption(predChannelTypeOption);
        options.addOption(statePortOption);
        options.addOption(tokenOption);

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

        String[] sports = line.getOptionValues("ports");
        Map<String, Integer> pelletPortMap = new HashMap<>();
        for (String pport: sports) {
            String[] sp = pport.split(":");
            String pellet = sp[0];
            String port = sp[1];
            pelletPortMap.put(pellet, Integer.parseInt(port));
        }

        String[] bsports = line.getOptionValues("backchannelports");
        Map<String, Integer> pelletBackChannelPortMap = new HashMap<>();
        for (String pport: bsports) {
            String[] sp = pport.split(":");
            String pellet = sp[0];
            String port = sp[1];
            pelletBackChannelPortMap.put(pellet, Integer.parseInt(port));
        }

        String[] sChannelTypes = line.getOptionValues("channeltype");
        Map<String, String> pelletChannelTypeMap = new HashMap<>();
        for (String ctype: sChannelTypes) {
            String[] sp = ctype.split(":");
            String pellet = sp[0];
            String type = sp[1];
            pelletChannelTypeMap.put(pellet, type);
        }

        String[] spredChannelTypes = line.getOptionValues("predchanneltype");
        Map<String, String> predPelletChannelTypeMap = new HashMap<>();
        for (String ctype: spredChannelTypes) {
            String[] sp = ctype.split(":");
            String pellet = sp[0];
            String type = sp[1];
            predPelletChannelTypeMap.put(pellet, type);
        }

        String[] streams = line.getOptionValues("streams");
        Map<String, List<String>> pelletStreamsMap = new HashMap<>();
        for (String sStreams: streams) {
            String[] sp = sStreams.split(":");
            String pellet = sp[0];
            List<String> streamNames = null;
            if (sp.length > 1) {
                streamNames = parseCSV(sp[1]);
            }
            pelletStreamsMap.put(pellet, streamNames);
        }

        Integer statePort = Integer.parseInt(line.getOptionValue("stateport"));

        LOGGER.info("pid: {}, id:{}, cid:{}, app:{}, jar:{}, ports:{}, "
            + "backports:{}, stream:{}, channeltype:{}, predChanneltype:{}",
            pid, id,
            cid,
            appName, jar, pelletPortMap, pelletBackChannelPortMap,
            pelletStreamsMap,
            pelletChannelTypeMap, predPelletChannelTypeMap);
        try {
            new FlakeService(pid,
                    id,
                    cid,
                    appName,
                    jar,
                    statePort,
                    pelletPortMap,
                    pelletBackChannelPortMap,
                    pelletChannelTypeMap,
                    predPelletChannelTypeMap,
                    pelletStreamsMap, token).start();
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
