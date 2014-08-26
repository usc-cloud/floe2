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

import edu.usc.pgroup.floe.container.ContainerInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Resource cache. Maintains a cached list of available resources.
 * @author kumbhare
 */
public final class ResourceMonitor {

    /**
     * ZK Container info cache.
     */
    private ZKContainersCache containersCache;

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ResourceMonitor.class);

    /**
     * Singleton instance.
     */
    private static ResourceMonitor instance;

    /**
     * Hiding the default constructor.
     */
    private ResourceMonitor() {
        initializeContainerMonitor();
        initializePerfMonitor();
        //Add custom hooks?
    }


    /**
     * @return singleton instance of resource monitor.
     */
    public static synchronized ResourceMonitor getInstance() {
        if (instance == null) {
            instance = new ResourceMonitor();
        }
        return instance;
    }


    /**
     * Initializes a cached performance monitor using ZKeeper.
     */
    private void initializePerfMonitor() {

    }


    /**
     * Initializes the ZK cache to monitor containers being added or removed.
     */
    private void initializeContainerMonitor() {
        LOGGER.info("Starting container monitor and cache.");
        containersCache = new ZKContainersCache(null);
    }

    /**
     * Gets the cached container data (including available cores, perf. etc.).
     * @return a list of available containers.
     */
    public List<ContainerInfo> getAvailableContainers() {
        return containersCache.getAvailableContainers();
    }
}
