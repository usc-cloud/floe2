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

package edu.usc.pgroup.floe.flake.coordination;

import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.app.pellets.IteratorPellet;
import edu.usc.pgroup.floe.flake.statemanager.StateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * @author kumbhare
 */
public final class PeerCoordinationManagerFactory {
    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PeerCoordinationManagerFactory.class);

    /**
     * Hiding default constructor.
     */
    private PeerCoordinationManagerFactory() {

    }

    /**
     * Constructs the state manager based on the pellet type.
     * @param metricRegistry Metric registry.
     * @param appName           the application name.
     * @param pelletName        pellet's name to which this flake belongs.
     * @param pellet The pellet object, used to figure out type of state
     *               manager to create.
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param stateManager the state manager associated with this flake.
     * @param ctx           Shared zmq context.
     * @return the instantiated (but not started) state manager object.
     */
    public static PeerCoordinationComponent getCoordinationManager(
            final MetricRegistry metricRegistry,
            final String appName,
            final String pelletName,
            final IteratorPellet pellet,
            final String flakeId,
            final String componentName,
            final StateManager stateManager,
            final ZMQ.Context ctx) {
        PeerCoordinationComponent manager = null;
        /*if (pellet instanceof ReducerPellet) {
            LOGGER.info("Reducer pellet. Creating reducer state manager.");
            int tolerance = FloeConfig.getConfig().getInt(
                    ConfigProperties.FLAKE_TOLERANCE_LEVEL);
            manager =  new ReducerPeerCoordinationComponent(
                            metricRegistry,
                            appName,
                            pelletName,
                            flakeId,
                            componentName,
                            ctx,
                            tolerance,
                            stateManager);
        } else  {
            manager = null;
        }*/

        return manager;
    }
}
