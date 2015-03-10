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

/**
 * The stateless Pellet interface which is the basic unit of user code.
 *
 * @author kumbhare
 */
public abstract class StatelessPellet implements Pellet {

    /**
     * The execute method which is called for each tuple.
     *
     * @param t       input tuple received from the preceding pellet.
     * @param emitter An output emitter which may be used by the user to emmit
     *                results.
     * @param state   state associated with the current execution of the pellet.
     */
    @Override
    public final void execute(final Tuple t,
                              final Emitter emitter,
                              final PelletState state) {
        //ignores state completely.
        execute(t, emitter);
    }

    /**
     * The execute method which is called for each tuple. (stateless)
     *
     * @param t       input tuple received from the preceding pellet.
     * @param emitter An output emitter which may be used by the user to emmit
     *                results.
     */
    protected abstract void execute(Tuple t, Emitter emitter);
}
