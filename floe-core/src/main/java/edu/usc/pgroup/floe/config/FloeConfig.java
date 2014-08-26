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

package edu.usc.pgroup.floe.config;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.DefaultConfigurationBuilder;

import java.io.File;

/**
 * Singleton FloeConfig class.
 *
 * @author Alok Kumbhare
 */
public final class FloeConfig {

    /**
     * The singleton class instance.
     */
    private static Configuration config;

    /**
     * Hiding the default constructor from outside world.
     */
    private FloeConfig() {

    }

    /**
     * To retrieve the config object.
     * FIXME: This is not a Thread Safe Function (for the first initialization)
     *
     * @return Fully initialized configuration object
     * (instance of Apache Commons Configuration)
     */
    public static Configuration getConfig() {
        if (config == null) {
            DefaultConfigurationBuilder builder =
                    new DefaultConfigurationBuilder();
            builder.setFile(new File("config.xml"));
            try {
                config = builder.getConfiguration(true);
            } catch (ConfigurationException e) {
                throw new RuntimeException(
                        "Missing configuration description file.");
            }
        }
        return config;
    }
}
