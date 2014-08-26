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

package edu.usc.pgroup.floe.resourcemanager;


import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kumbhare
 */
public final class ResourceManagerFactory {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ResourceManagerFactory.class);

    /**
     * Hiding the default constructor.
     */
    private ResourceManagerFactory() {

    }

    /**
     * @return the instance of the configured resource manager.
     */
    public static ResourceManager getResourceManager() {
        ResourceManager resManager = null;


        String resManagerClass = FloeConfig.getConfig().getString(
                ConfigProperties.FLOE_EXE_RESOURCE_MANAGER,
                        "edu.usc.pgroup.floe.resourcemanager"
                                + ".ClusterResourceManager");

        LOGGER.info("Initializing resource manager:" + resManagerClass);
        try {
            Class resClass = Class.forName(resManagerClass);
            resManager = (ResourceManager) resClass.newInstance();
        } catch (ClassNotFoundException e) {
            LOGGER.warn("Could not initialize resource manager class. Using "
                    + "default resource manager. Exception: {}", e);
            resManager = new ClusterResourceManager();
        } catch (InstantiationException e) {
            LOGGER.warn("Could not initialize resource manager class. Using "
                    + "default resource manager. Exception: {}", e);
            resManager = new ClusterResourceManager();
        } catch (IllegalAccessException e) {
            LOGGER.warn("Could not initialize resource manager class. Using "
                    + "default resource manager. Exception: {}", e);
            resManager = new ClusterResourceManager();
        }

        return resManager;
    }
}
