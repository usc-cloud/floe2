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
import edu.usc.pgroup.floe.examples.pellets.WordPellet;
import edu.usc.pgroup.floe.utils.Utils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public final class SimpleDynamicApp {

    /**
     * Time for which to run the application.
     */
    private static final int APP_RUNNING_TIME = 100;

    /**
     * Time for which to run the application.
     */
    private static final double WELCOME_GREET_VALUE = 0.5;


    /**
     * Time for which to run the application.
     */
    private static final double HELLO_GREET_VALUE = 1.0;


    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SimpleDynamicApp.class);

    /**
     * Hiding the public constructor.
     */
    private SimpleDynamicApp() {

    }

    /**
     * Sample main.
     * @param args commandline args.
     */
    public static void main(final String[] args) {
        System.out.println("Hello World!");
        ApplicationBuilder builder = new ApplicationBuilder();

        String[] words = {"John", "Jane", "Maverick", "Alok"};

        builder.addPellet("word", new WordPellet(words)).setParallelism(1);

        builder.addDynamicPellet("greeting")
                .addAlternate("hello", HELLO_GREET_VALUE
                        , new HelloGreetingPellet())
                .addAlternate("welcome", WELCOME_GREET_VALUE
                        , new WelcomeGreetingPellet())
                .setActiveAlternate("welcome")
                .subscribe("word").setParallelism(1);

        try {
            AppSubmitter.submitApp("helloworld", builder.generateApp());
        } catch (TException e) {
           LOGGER.error("Error while deploying app. Exception {}", e);
        }


        if (FloeConfig.getConfig().getString(ConfigProperties.FLOE_EXEC_MODE)
                .equalsIgnoreCase("local")) {
            try {
                Thread.sleep(APP_RUNNING_TIME * Utils.Constants.MILLI);
                AppSubmitter.shutdown();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        } else {
            LOGGER.info("Application submitted.");
        }


    }
}
