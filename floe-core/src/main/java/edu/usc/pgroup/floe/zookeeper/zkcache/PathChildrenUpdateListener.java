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

import java.util.Collection;

/**
 * Higher level abstraction for curator's PathChildCache Listener.
 *
 * @author kumbhare
 */
public interface PathChildrenUpdateListener {
    /**
     * Triggered when initial list of children is cached.
     * This is retrieved synchronously.
     *
     * @param initialChildren initial list of children.
     */
    void childrenListInitialized(Collection<ChildData> initialChildren);

    /**
     * Triggered when a new child is added.
     * Note: this is not recursive.
     *
     * @param addedChild newly added child's data.
     */
    void childAdded(ChildData addedChild);

    /**
     * Triggered when an existing child is removed.
     * Note: this is not recursive.
     *
     * @param removedChild removed child's data.
     */
    void childRemoved(ChildData removedChild);

    /**
     * Triggered when a child is updated.
     * Note: This is called only when Children data is also cached in
     * addition to stat information.
     *
     * @param updatedChild update child's data.
     */
    void childUpdated(ChildData updatedChild);
}
