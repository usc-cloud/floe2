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

package edu.usc.pgroup.floe.zookeeper.zkcache;

import edu.usc.pgroup.floe.resourcemanager.ResourceMonitor;
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author kumbhare
 */
public class ZKChildrenCache {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ResourceMonitor.class);

    /**
     * The cached path.
     */
    private final String cachedPath;

    /**
     * the children cache listener.
     */
    private final PathChildrenUpdateListener cacheListener;

    /**
     * Flag indicating if the cache should include data.
     */
    private final boolean cacheData;

    /**
     * Container cache.        //Add custom hooks?
     * Note: This is currently not used since we do all operation on events.
     */
    private PathChildrenCache pathCache;

    /**
     * Default constructor.
     * @param path the ZK path to cache and listen for changes.
     * @param listener the update listener callback.
     */
    public ZKChildrenCache(final String path,
                           final PathChildrenUpdateListener listener) {
        this(path, listener, true);
    }

    /**
     * Default constructor.
     * @param path the ZK path to cache and listen for changes.
     * @param listener the update listener callback.
     * @param data flag indicating if the cache should contain data (or
     *                  just a list)
     */
    public ZKChildrenCache(final String path,
                           final PathChildrenUpdateListener listener,
                           final boolean data) {
        this.cachedPath = path;
        this.cacheListener = listener;
        this.cacheData = data;
        initializeContainerMonitor();
    }

    /**
     * Initializes the ZK cache to monitor containers being added or removed.
     */
    private void initializeContainerMonitor() {
        LOGGER.info("Subscribing for updates at path:"
                + cachedPath);

        pathCache = ZKClient.getInstance().cacheAndSubscribeChildren(
                cachedPath,
                cacheListener,
                cacheData
                //TODO: For perf. reasons We should not caching container
                // data. That will be received on demand. We just want to cache
                // container list.
        );

    }

    /**
     * @param listener the update listener callback.
     */
    public final void addUpdateListener(
            final PathChildrenUpdateListener listener) {
        ZKClient.getInstance().addPathCacheListener(pathCache, listener);
    }

    /**
     * @return the current cached data, including children data.
     */
    public final List<ChildData> getCurrentCachedData() {
        return pathCache.getCurrentData();
    }

    /**
     * Rebuild the cache.
     */
    public final void rebuild() {
        try {
            pathCache.rebuild();
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.warn("Could not retrieve path cache. {}", e);
        }
    }
}
