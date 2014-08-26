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

import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;

/**
 * Update Listener for list of containers.
 *
 * @author kumbhare
 */
public class BaseChildrenCacheListener implements PathChildrenUpdateListener {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(BaseChildrenCacheListener.class);

    /**
     * Triggered when initial list of children is cached.
     * This is retrieved synchronously.
     *
     * @param initialChildren initial list of children.
     */
    @Override
    public final void childrenListInitialized(
            final Collection<ChildData> initialChildren) {
        LOGGER.info("Initial container list: ");
        for (ChildData child : initialChildren) {
            LOGGER.info(ZKPaths.getNodeFromPath(child.getPath()));
        }
    }

    /**
     * Triggered when a new child is added.
     * Note: this is not recursive.
     *
     * @param addedChild newly added child's data.
     */
    @Override
    public final void childAdded(final ChildData addedChild) {
        LOGGER.info("Container Added: "
                + ZKPaths.getNodeFromPath(addedChild.getPath()));
    }

    /**edu.usc.pgroup
     * Triggered when an existing child is removed.
     * Note: this is not recursive.
     *
     * @param removedChild removed child's data.
     */
    @Override
    public final void childRemoved(final ChildData removedChild) {
        LOGGER.info("Container Removed: "
                + ZKPaths.getNodeFromPath(removedChild.getPath()));
    }

    /**
     * Triggered when a child is updated.
     * Note: This is called only when Children data is also cached in
     * addition to stat information.
     *
     * @param updatedChild update child's data.
     */
    @Override
    public final void childUpdated(final ChildData updatedChild) {
        LOGGER.info("Container Updated: "
                + ZKPaths.getNodeFromPath(updatedChild.getPath()));
    }
}
