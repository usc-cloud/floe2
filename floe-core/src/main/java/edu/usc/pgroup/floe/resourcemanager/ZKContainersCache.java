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
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKConstants;
import edu.usc.pgroup.floe.zookeeper.zkcache.PathChildrenUpdateListener;
import edu.usc.pgroup.floe.zookeeper.zkcache.ZKChildrenCache;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kumbhare
 */
public class ZKContainersCache extends ZKChildrenCache {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ZKContainersCache.class);

    /**
     * constructor.
     * @param listener the update listener callback.
     */
    public ZKContainersCache(final PathChildrenUpdateListener listener) {
        super(ZKConstants.Container.ROOT_NODE, listener);
    }

    /**
     * Gets the cached container data (including available cores, perf. etc.).
     * @return a list of available containers.
     */
    public final List<ContainerInfo> getAvailableContainers() {
        List<ChildData> children = getCurrentCachedData();
        List<ContainerInfo> containers = new ArrayList<>();

        for (ChildData child: children) {
            byte[] ser = child.getData();
            ContainerInfo info = (ContainerInfo) Utils.deserialize(ser);
            containers.add(info);
        }
        return containers;
    }
}
