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

import edu.usc.pgroup.floe.app.Pellet;
import edu.usc.pgroup.floe.app.ReducerPellet;
import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.flake.statemanager.StateManagerComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * @author kumbhare
 */
public final class CoordinationManagerFactory {
    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(CoordinationManagerFactory.class);

    /**
     * Hiding default constructor.
     */
    private CoordinationManagerFactory() {

    }

    /**
     * Constructs the state manager based on the pellet type.
     * @param appName           the application name.
     * @param pelletName        pellet's name to which this flake belongs.
     * @param pellet The pellet object, used to figure out type of state
     *               manager to create.
     * @param flakeId       Flake's id to which this component belongs.
     * @param myToken       This flake's current token value.
     * @param componentName Unique name of the component.
     * @param stateManager the state manager associated with this flake.
     * @param ctx           Shared zmq context.
     * @return the instantiated (but not started) state manager object.
     */
    public static CoordinationComponent getCoordinationManager(
            final String appName,
            final String pelletName,
            final Pellet pellet,
            final String flakeId,
            final Integer myToken,
            final String componentName,
            final StateManagerComponent stateManager,
            final ZMQ.Context ctx) {
        CoordinationComponent manager = null;
        if (pellet instanceof ReducerPellet) {
            LOGGER.info("Reducer pellet. Creating reducer state manager.");
            int tolerance = FloeConfig.getConfig().getInt(
                    ConfigProperties.FLAKE_TOLERANCE_LEVEL);
            manager =  new ReducerCoordinationComponent(
                            appName,
                            pelletName,
                            flakeId,
                            myToken,
                            componentName,
                            ctx,
                            tolerance,
                            stateManager);
        } else  {
            manager = null;
        }

        return manager;
    }
}
