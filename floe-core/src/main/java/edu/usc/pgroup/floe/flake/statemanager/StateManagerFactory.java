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

package edu.usc.pgroup.floe.flake.statemanager;

import edu.usc.pgroup.floe.app.Pellet;
import edu.usc.pgroup.floe.app.ReducerPellet;
import edu.usc.pgroup.floe.app.StatefulPellet;
import edu.usc.pgroup.floe.app.StatelessPellet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * @author kumbhare
 */
public final class StateManagerFactory {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(StateManagerFactory.class);

    /**
     * Hiding default constructor.
     */
    private StateManagerFactory() {

    }

    /**
     * Constructs the state manager based on the pellet type.
     *
     * @param pellet The pellet object, used to figure out type of state
     *               manager to create.
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     * @return the instantiated (but not started) state manager object.
     */
    public static StateManagerComponent getStateManager(
            final Pellet pellet,
            final String flakeId,
            final String componentName,
            final ZMQ.Context ctx) {
        StateManagerComponent manager = null;
        if (pellet instanceof StatelessPellet) {
            LOGGER.info("Stateless pellet. No state required.");
            manager = null;
        } else if (pellet instanceof ReducerPellet) {
            LOGGER.info("Reducer pellet. Creating reducer state manager.");
            String fieldName = ((ReducerPellet) pellet).getKeyFieldName();
            manager =  new ReducerStateManager(
                    flakeId, componentName, ctx, fieldName);
        } else if (pellet instanceof StatefulPellet) {
            LOGGER.info("regular Statefull pellet. Creating pellet state "
                    + "manager.");
            manager =  new PelletStateManager(flakeId, componentName, ctx);
        }

        return manager;
    }
}
