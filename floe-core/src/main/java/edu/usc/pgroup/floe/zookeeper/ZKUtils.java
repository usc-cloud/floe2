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

package edu.usc.pgroup.floe.zookeeper;

import org.apache.curator.utils.ZKPaths;


/**
 * Floe specific utility functions for zookeeper.
 *
 * @author kumbhare
 */
public final class ZKUtils {

    /**
     * Hiding the default constructor.
     */
    private ZKUtils() {

    }

    /**
     * @param containerId Container Id (i.e. the node name for the container)
     * @return the full path for the container node.
     */
    public static String getContainerPath(final String containerId) {
        return ZKPaths.makePath(
                ZKConstants.Container.ROOT_NODE,
                containerId
        );
    }

    /**
     * @return the root folder path for application related data.
     */
    public static String getApplicationRootPath() {
        return ZKPaths.makePath(ZKConstants.Coordinator.ROOT_NODE,
                ZKConstants.Coordinator.APP_NODE);
    }

    /**
     * @param appName the application name.
     * @return the full path for the application's root node.
     */
    public static String getApplicationPath(final String appName) {
        return ZKPaths.makePath(
            getApplicationRootPath(),
            appName
        );
    }
}
