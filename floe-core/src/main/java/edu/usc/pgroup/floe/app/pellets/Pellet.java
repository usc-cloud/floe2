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

import edu.usc.pgroup.floe.app.AppContext;
import edu.usc.pgroup.floe.app.Emitter;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.flake.statemanager.PelletState;

import java.io.Serializable;
import java.util.List;

/**
 * The Pellet common interface which is the basic unit of user code.
 * Stateful and stateless pellets are derived from this common.
 * @author kumbhare
 */
public interface Pellet extends Serializable {
    /**
     * The setup function is called once to let the pellet initialize.
     * @param appContext Application's context. Some data related to
     *                   application's deployment.
     * @param pelletContext Pellet instance context. Related to this
     *                      particular pellet instance.
     */
    void setup(AppContext appContext, PelletContext pelletContext);


    /**
     * The onStart function is called once just before executing the pellet
     * and after the setup function. Typically, this is used by a data source
     * pellet which does not depend on external data source but generates
     * tuples on its own.
     * @param emitter An output emitter which may be used by the user to emmit
     *                results.
     */
    void onStart(Emitter emitter);

    /**
     * The teardown function, called when the topology is killed.
     * Or when the Pellet instance is scaled down.
     */
    void teardown();

    /**
     * @return The names of the streams to be used later during emitting
     * messages.
     */
    List<String> getOutputStreamNames();

    /**
     * The execute method which is called for each tuple.
     *
     * @param t       input tuple received from the preceding pellet.
     * @param emitter An output emitter which may be used by the user to emmit
     *                results.
     * @param state state associated with the current execution of the pellet.
     */
    void execute(Tuple t, Emitter emitter, PelletState state);
}
