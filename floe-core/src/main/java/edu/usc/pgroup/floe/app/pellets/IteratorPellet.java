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
import edu.usc.pgroup.floe.flake.statemanager.StateManager;

import java.io.Serializable;
import java.util.List;

/**
 * The Pellet common interface which is the basic unit of user code.
 * Stateful and stateless pellets are derived from this common.
 * @author kumbhare
 */
public abstract class IteratorPellet implements Serializable {

    /**
     * Pellet configuration (such as number of parallel instances etc).
     */
    private PelletConfiguration conf;

    /**
     * Indicates whether the pellet has been started.
     */
    private boolean started;

    /**
     * Default constructor.
     */
    public IteratorPellet() {
        conf = new PelletConfiguration();
        started = false;
    }


    /**
     * Initializes the pellet.
     */
    public final void init() {
        configure(this.conf);
    }

    /**
     * @return the pellet configuration.
     */
    public final PelletConfiguration getConf() {
        return conf;
    }

    /**
     * Use to configure different aspects of the pellet,such as state type etc.
     * @param pconf pellet configurer
     */
    public abstract void configure(final PelletConfiguration pconf);


    /**
     * The setup function is called once to let the pellet initialize.
     * @param appContext Application's context. Some data related to
     *                   application's deployment.
     * @param pelletContext Pellet instance context. Related to this
     *                      particular pellet instance.
     */
    public abstract void onStart(final AppContext appContext,
                                 final PelletContext pelletContext);

    /**
     * The teardown function, called when the topology is killed.
     * Or when the Pellet instance is scaled down.
     */
    public abstract void teardown();

    /**
     * @return The names of the streams to be used later during emitting
     * messages.
     */
    public abstract List<String> getOutputStreamNames();

    /**
     * The execute method which is called for each tuple.
     *
     * @param tupleItertaor      input tuple received from the preceding pellet.
     * @param emitter An output emitter which may be used by the user to emmit
     *                results.
     * @param stateManager state associated manager associated with the pellet.
     *                     It is the executor's responsiblity to get the state
     *              associated with the tuple.
     */
    public abstract void execute(final TupleItertaor tupleItertaor,
                                 final Emitter emitter,
                                 final StateManager stateManager);

    /**
     * @return returns true if the pellet has been started. False otherwise
     */
    public final boolean hasStarted() {
        return started;
    }

    /**
     * Marks the pellet as started.
     */
    public final void markStarted() {
        this.started = true;
    }
}
