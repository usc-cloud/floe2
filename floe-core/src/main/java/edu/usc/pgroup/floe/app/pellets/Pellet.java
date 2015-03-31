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

package edu.usc.pgroup.floe.app.pellets;

import edu.usc.pgroup.floe.app.Emitter;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.flake.statemanager.PelletState;
import edu.usc.pgroup.floe.flake.statemanager.StateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The Pellet common interface which is the basic unit of user code.
 * Stateful and stateless pellets are derived from this common.
 * @author kumbhare
 */
public abstract class Pellet extends IteratorPellet {
    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(TupleItertaor.class);

    /**
     * The execute method which is called for each tuple.
     *
     * @param t       input tuple received from the preceding pellet.
     * @param emitter An output emitter which may be used by the user to emmit
     *                results.
     * @param state state associated with the current execution of the pellet.
     */
    public abstract void execute(final Tuple t,
                                 final Emitter emitter,
                                 final PelletState state);

    /**
     * The execute method which is called for each tuple.
     *
     * @param tupleItertaor       input tuple received from the preceding
     *                            pellet.
     * @param emitter An output emitter which may be used by the user to emmit
     *                results.
     * @param stateMgr state associated manager associated with the pellet.
     *                     It is the executor's responsiblity to get the state
     *              associated with the tuple.
     */
    @Override
    public final void execute(final TupleItertaor tupleItertaor,
                        final Emitter emitter,
                        final StateManager stateMgr) {
        Tuple t = null;
        if (tupleItertaor != null && !getConf().isSourcePellet()) {
            t = tupleItertaor.next();
        }
        PelletState state = null;
        if (stateMgr != null) {
            stateMgr.getState(tupleItertaor.getPeId(), t);
        }
        execute(t, emitter, state);
    }
}
