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

package edu.usc.pgroup.floe.flake;

import edu.usc.pgroup.floe.zookeeper.zkcache.PathChildrenUpdateListener;
import edu.usc.pgroup.floe.zookeeper.zkcache.ZKChildrenCache;

/**
 * @author kumbhare
 */
public class ZKFlakeTokenCache extends ZKChildrenCache {


    /**
     * Default constructor.
     *
     * @param path     the ZK path to cache and listen for changes.
     * @param listener the update listener callback.
     */
    public ZKFlakeTokenCache(final String path,
                             final PathChildrenUpdateListener listener) {
        super(path, listener);
    }
}
