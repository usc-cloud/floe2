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

package edu.usc.pgroup.floe.container;

import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import edu.usc.pgroup.floe.zookeeper.zkcache.PathChildrenUpdateListener;
import edu.usc.pgroup.floe.zookeeper.zkcache.ZKChildrenCache;

/**
 * @author kumbhare
 */
public class ZKAppsCache extends ZKChildrenCache {

    /**
     * The container id.
     */
    private final String containerId;

    /**
     * constructor.
     * @param listener the update listener callback.
     * @param cid the container id who is listening for app changes.
     */
    public ZKAppsCache(final PathChildrenUpdateListener listener,
                       final String cid) {
        super(ZKUtils.getApplicationRootPath(), listener);
        this.containerId = cid;
    }

}
