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

package edu.usc.pgroup.floe.app.signals;

import edu.usc.pgroup.floe.container.FlakeControlCommand;
import edu.usc.pgroup.floe.container.FlakeControlSignalSender;
import edu.usc.pgroup.floe.container.FlakeMonitor;
import edu.usc.pgroup.floe.flake.FlakeInfo;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.zkcache.PathChildrenUpdateListener;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;

/**
 * @author kumbhare
 */
public class SignalMonitor {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SignalMonitor.class);

    /**
     * ZK applications assignment cache.
     */
    private ZKSignalsCache appsCache;

    /**
     * container id.
     */
    private String containerId;

    /**
     * constructor.
     * @param cid container id.
     */
    public SignalMonitor(final String cid) {
        appsCache = new ZKSignalsCache(new SignalListener(),
                cid);
        this.containerId = cid;
    }

    /**
     * Apps assignment listener.
     */
    public class SignalListener implements PathChildrenUpdateListener {

        /**
         * Triggered when initial list of children is cached.
         * This is retrieved synchronously.
         *
         * @param initialChildren initial list of children.
         */
        @Override
        public void childrenListInitialized(
                final Collection<ChildData> initialChildren) {
        }

        /**
         * Triggered when a new child is added.
         * Note: this is not recursive.
         *
         * @param addedChild newly added child's data.
         */
        @Override
        public final void childAdded(final ChildData addedChild) {
            //New signal received is added.
            //Check for any assignments made to this container and start
            // flakes as required.
            //We still need to check for existing Flakes since multiple
            // pellet instances for a given pellet might be needed.
            byte[] ser = addedChild.getData();

            processSignal(ser);
        }

        /**
         * Triggered when an existing child is removed.
         * Note: this is not recursive.
         *
         * @param removedChild removed child's data.
         */
        @Override
        public void childRemoved(final ChildData removedChild) {

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
            byte[] ser = updatedChild.getData();
            processSignal(ser);
        }


        /**
         * processes the received signal.
         * @param ser serialized signal received from ZK.
         */
        private void processSignal(final byte[] ser) {
            Map<String, FlakeInfo> runningFlakes
                    = FlakeMonitor.getInstance().getFlakes();

            Signal signal = (Signal) Utils.deserialize(ser);

            for (FlakeInfo info: runningFlakes.values()) {
                if (info.getAppName().equalsIgnoreCase(
                        signal.getDestApp())
                        && info.getPelletId().equalsIgnoreCase(
                        signal.getDestPellet())) {

                    FlakeControlCommand command = new FlakeControlCommand(
                            FlakeControlCommand.Command.PELLET_SIGNAL, ser);

                    FlakeControlSignalSender.getInstance().send(
                            info.getFlakeId(),
                            command
                    );
                }
            }
        }
    }
}
