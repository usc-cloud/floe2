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

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;

/**
 * Zookeeper paths and constants for various services.
 *
 * @author Alok Kumbhares
 */
public final class ZKConstants {

    /**
     * Floe root in ZK.
     * Note: This is used while connecting to zookeeper. It should not be used
     * in the ZK paths while accessing data. That will be automatically done.
     */
    public static final String FLOE_ROOT =
            FloeConfig.getConfig().getString(ConfigProperties.ZK_ROOT);

    /**
     * List of ZK servers.
     * FIXME: We assume the list of servers do not change over lifetime. This
     * might be ok.
     */
    public static final String[] SERVERS =
            FloeConfig.getConfig().getStringArray(ConfigProperties.ZK_SERVERS);

    /**
     * ZK Server PORT.
     */
    public static final int PORT = FloeConfig.getConfig().getInt(
            ConfigProperties.ZK_PORT);

    /**
     * Hiding the public constructor.
     */
    private ZKConstants() {

    }

    /**
     * ZK Paths for Coordinator.
     */
    public static class Coordinator {
        /**
         * Coordinator node.
         */
        public static final String ROOT_NODE = "/coordinator";

        /**
         * The Application root node, relative to the coordinator
         * for the applications.
         */
        public static final String APP_NODE = "apps";

        /**
         * The root node to keep terminated apps.
         */
        public static final String TERMINATED_APP_NODE = "terminated";

        /**
         * The signals root node, relative to the coordinator.
         */
        public static final String SIGNAL_NODE = "signals";

        /**
         * The location, relative to the coordinator,
         * where the cluster's status is stored.
         */
        public static final String CLUSTER_STATUS = "status";

        /**
         * The location, relative to the application's node where the app's
         * status is stored.
         */
        public static final String APP_STATUS  = CLUSTER_STATUS;

        /**
         * the location, relative to the specific application's node,
         * where the corresponding resource mapping is stored.
         */
        public static final String RESOURCE_MAP = "resourcemap";

        /**
         * the location, relative to the specific application's node,
         * which will be used for barrier synchronization.
         */
        public static final String APP_BARRIER = "barrier";

        /**
         * the location, relative to the specific application's node,
         * which will be used for barrier synchronization.
         */
        public static final String APP_DATA = "appdata";

        /**
         * the location, relative to the specific application's node,
         * which will be used for barrier synchronization.
         */
        public static final String APP_FLAKE_TOKENS = "tokens";
    }

    /**
     * ZK Paths for container.
     */
    public static class Container {
        /**
         * Coordinator node.
         */
        public static final String ROOT_NODE = "/containers";
    }

}
