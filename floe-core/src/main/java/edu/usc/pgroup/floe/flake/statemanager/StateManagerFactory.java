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

import edu.usc.pgroup.floe.app.pellets.IteratorPellet;
import edu.usc.pgroup.floe.app.pellets.Pellet;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
     * @param pellet The pellet object, used to figure out type of state
     *               manager to create.
     * @return the instantiated (but not started) state manager object.
     */
    public static StateManager getStateManager(final IteratorPellet pellet) {

        StateManager manager = null;
        String stateManagerClass = pellet.getConf().getStateManagerClass();
        if (stateManagerClass != null) {
            LOGGER.error("State manager: " + stateManagerClass);
            manager = (StateManager) Utils.instantiateObject(stateManagerClass);
        }

        if (manager != null) {
            manager.init(pellet.getConf().getStateParams());
        }
        return manager;
    }
}
