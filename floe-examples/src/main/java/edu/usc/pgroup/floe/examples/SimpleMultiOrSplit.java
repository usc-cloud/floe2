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

package edu.usc.pgroup.floe.examples;

import edu.usc.pgroup.floe.app.ApplicationBuilder;
import edu.usc.pgroup.floe.client.AppSubmitter;
import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.examples.pellets.HelloGreetingPellet;
import edu.usc.pgroup.floe.examples.pellets.WelcomeGreetingPellet;
import edu.usc.pgroup.floe.examples.pellets.WordMultiStreamPellet;
import edu.usc.pgroup.floe.utils.Utils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kumbhare
 */
public final class SimpleMultiOrSplit {
    /**
     * Time for which to run the application.
     */
    private static final int APP_RUNNING_TIME = 100;

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SimpleAndSplit.class);

    /**
     * Hiding the public constructor.
     */
    private SimpleMultiOrSplit() {

    }

    /**
     * Sample main.
     * @param args commandline args.
     */
    public static void main(final String[] args) {
        System.out.println("Hello World!");
        ApplicationBuilder builder = new ApplicationBuilder();

        String[] boys = {"John", "James", "Sherlock", "Moriarty", "Alok"};
        String[] girls = {"Irene", "Molly", "Jasmine", "Julie", "Monica"};

        builder.addPellet("names",
                new WordMultiStreamPellet("boys", "girls",
                        boys, girls)).setParallelism(1);

        builder.addPellet("hello", new HelloGreetingPellet())
                .subscribe("names", "boys").setParallelism(1);

        builder.addPellet("welcome", new WelcomeGreetingPellet())
                .subscribe("names", "girls", "boys").setParallelism(1);

        try {
            AppSubmitter.submitApp("helloworld", builder.generateApp());
        } catch (TException e) {
            LOGGER.error("Error while deploying app. Exception {}", e);
        }

        if (FloeConfig.getConfig().getString(ConfigProperties.FLOE_EXEC_MODE)
                .equalsIgnoreCase("local")) {
            try {
                Thread.sleep(APP_RUNNING_TIME * Utils.Constants.MILLI);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            AppSubmitter.shutdown();
        }
    }
}
