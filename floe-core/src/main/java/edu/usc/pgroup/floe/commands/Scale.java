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

package edu.usc.pgroup.floe.commands;

import edu.usc.pgroup.floe.client.FloeClient;
import edu.usc.pgroup.floe.thriftgen.ScaleDirection;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.thrift.TException;
import org.slf4j.LoggerFactory;

/**
 * @author kumbhare
 */
public final class Scale {


    /**
     * Hiding the default constructor.
     */
    private Scale() {

    }

    /**
     * the global logger instance.
     */
    private static final org.slf4j.Logger LOGGER =
            LoggerFactory.getLogger(Scale.class);

    /**
     * Entry point for Scale command.
     * @param args command line arguments sent by the floe.py script.
     */
    public static void main(final String[] args) {

        Options options = new Options();

        Option dirOption = OptionBuilder.withArgName("direction")
                .hasArg().isRequired()
                .withDescription("Scale Direction.")
                .create("dir");

        Option appOption = OptionBuilder.withArgName("name")
                .hasArg().isRequired()
                .withDescription("Application Name")
                .create("app");

        Option pelletNameOption = OptionBuilder.withArgName("name")
                .hasArg().isRequired()
                .withDescription("Pellet Name")
                .create("pellet");

        Option cntOption = OptionBuilder.withArgName("num")
                .hasArg().withType(new String())
                .withDescription("Number of instances to scale up/down")
                .create("cnt");

        options.addOption(dirOption);
        options.addOption(appOption);
        options.addOption(pelletNameOption);
        options.addOption(cntOption);

        CommandLineParser parser = new BasicParser();
        CommandLine line;

        try {
            line = parser.parse(options, args);

        } catch (ParseException e) {
            LOGGER.error("Invalid command: " + e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp("scale options", options);
            return;
        }

        String dir = line.getOptionValue("dir");
        String app = line.getOptionValue("app");
        String pellet = line.getOptionValue("pellet");
        String cnt = line.getOptionValue("cnt");

        LOGGER.info("direction: {}", dir);
        LOGGER.info("Application: {}", app);
        LOGGER.info("Pellet: {}", pellet);
        LOGGER.info("count: {}", cnt);

        ScaleDirection direction = Enum.valueOf(ScaleDirection.class, dir);
        int count = Integer.parseInt(cnt);
        try {
            FloeClient.getInstance().getClient().scale(
                    direction, app, pellet, count
            );
        } catch (TException e) {
            LOGGER.error("Error while connecting to the coordinator: {}", e);
        }
    }
}
