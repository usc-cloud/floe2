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

package edu.usc.pgroup.floe.app;

import edu.usc.pgroup.floe.flake.statemanager.PelletState;

/**
 * @author kumbhare
 */
public abstract class StatefulPellet implements Pellet {

    /**
     * Reducer specific execute function which is called for each input tuple
     * with the state corresponding to the key. This state will be persisted
     * across executes for each of the keys.
     * @param t input tuple received from the preceding pellet.
     * @param emitter An output emitter which may be used by the user to emmit.
     * @param state State specific to the key value given in the tuple.
     */
    public abstract void execute(Tuple t, Emitter emitter, PelletState state);
}
