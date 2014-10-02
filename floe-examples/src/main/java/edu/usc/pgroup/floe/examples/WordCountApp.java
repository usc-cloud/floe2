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
import edu.usc.pgroup.floe.examples.pellets.WordCountReducer;
import edu.usc.pgroup.floe.examples.pellets.WordPellet;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.utils.Utils;
import org.apache.commons.lang.RandomStringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kumbhare
 */
public final class WordCountApp {
    /**
     * Time for which to run the application.
     */
    private static final int APP_RUNNING_TIME = 100;

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(WordCountApp.class);

    /**
     * Hiding the public constructor.
     */
    private WordCountApp() {

    }

    /**
     * Sample main.
     * @param args commandline args.
     */
    public static void main(final String[] args) {
        System.out.println("Hello World!");
        ApplicationBuilder builder = new ApplicationBuilder();

        final int numWords = 20;
        final int maxWordLength = 3;
        final int numReducers = 2;
        final int numChars = 26;
        String[] words = new String[numWords];


        for (int i = 0; i < numWords; i++) {
            //words[i] = Character.toString((char) ('a' + i % numChars));
            words[i] = i + ":"
                    + RandomStringUtils.randomAlphabetic(maxWordLength);
        }
        //String[] words = {"John", "Jane", "Maverick", "Alok", "Jack"};

        builder.addPellet("words", new WordPellet(words)).setParallelism(1);

        builder.addPellet("count", new WordCountReducer("word"))
                .setParallelism(numReducers).reduce("words");

        TFloeApp app = builder.generateApp();
        try {
            AppSubmitter.submitApp("helloworld", app);
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
