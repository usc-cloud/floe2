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

package edu.usc.pgroup.floe.examples.pellets;

import edu.usc.pgroup.floe.app.AppContext;
import edu.usc.pgroup.floe.app.Emitter;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.app.pellets.IteratorPellet;
import edu.usc.pgroup.floe.app.pellets.Pellet;
import edu.usc.pgroup.floe.app.pellets.PelletConfiguration;
import edu.usc.pgroup.floe.app.pellets.PelletContext;
import edu.usc.pgroup.floe.app.pellets.Signallable;
import edu.usc.pgroup.floe.app.pellets.TupleItertaor;
import edu.usc.pgroup.floe.flake.statemanager.PelletState;
import edu.usc.pgroup.floe.flake.statemanager.StateManager;
import edu.usc.pgroup.floe.signals.PelletSignal;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author kumbhare
 */
public class PrintPellet extends IteratorPellet implements Signallable {

    /**
     * Pellet's instance id.
     */
    private String peInstanceId;

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PrintPellet.class);

    /**
     * Use to configure different aspects of the pellet,such as state type etc.
     *
     * @param conf pellet configurer
     */
    @Override
    public final void configure(final PelletConfiguration conf) {

    }

    /**
     * The setup function is called once to let the pellet initialize.
     * @param appContext Application's context. Some data related to
     *                   application's deployment.
     * @param pelletContext Pellet instance context. Related to this
     *                      particular pellet instance.
     */
    @Override
    public final void onStart(final AppContext appContext,
                      final PelletContext pelletContext) {
        this.peInstanceId = pelletContext.getPelletInstanceId();
    }


    /**
     * The execute method which is called for each tuple.
     *
     * @param t       input tuple received from the preceding pellet.
     * @param emitter An output emitter which may be used by the user to emmit

    @Override
    public final void execute(final Tuple t, final Emitter emitter) {
        if (t == null) {
            LOGGER.info("Dummy execute PRINT.");
        } else {
            LOGGER.info("{} : Received: {}", peInstanceId, t.get("word"));
            emitter.emit(t);
        }
    }*/

    /**
     * Reducer specific execute function which is called for each input tuple
     * with the state corresponding to the key. This state will be persisted
     * across executes for each of the keys.
     *
     * @param t       input tuple received from the preceding pellet.
     * @param emitter An output emitter which may be used by the user to emmit.
     * @param state   State specific to the key value given in the tuple.
     *
    @Override
    public final void execute(
            final Tuple t,
            final Emitter emitter,
            final PelletState state) {
        if (t == null) {
            LOGGER.info("Dummy execute PRINT.");
        } else {
            /*Object value = state.getValue("count");
            Integer count = 0;
            if (value != null) {
                count = (Integer) value + 1;
            }
            state.setValue("count", count);
            LOGGER.info("{} : {} : Received: {}",
                    peInstanceId,
                    count,
                    t.get("word"));*
            LOGGER.info("{} : Received: {}",
                    peInstanceId,
                    t.get("word"));
            emitter.emit(t);
        }
    }*/

    /**
     * The teardown function, called when the topology is killed.
     * Or when the Pellet instance is scaled down.
     */
    @Override
    public final void teardown() {

    }

    /**
     * @return The names of the streams to be used later during emitting
     * messages.
     */
    @Override
    public final List<String> getOutputStreamNames() {
        return null;
    }

    /**
     * The execute method which is called for each tuple.
     *
     * @param tupleItertaor input tuple received from the preceding pellet.
     * @param emitter       An output emitter which may be used by the user to emmit
     *                      results.
     * @param stateManager  state associated manager associated with the pellet.
     *                      It is the executor's responsiblity to get the state
     */
    @Override
    public void execute(final TupleItertaor tupleItertaor,
                        final Emitter emitter,
                        final StateManager stateManager) {
        while (true) {
            Tuple t = tupleItertaor.next();
            LOGGER.error("{} : Received: {}",
                    peInstanceId, t.get("word"));
        }
    }

    /**
     * Called when a signal is received for the component.
     *
     * @param signal the signal received for this pellet.
     */
    @Override
    public final void onSignal(final PelletSignal signal) {
        LOGGER.error("RECEIVED SIGNAL: {}",
                Utils.deserialize(signal.getSignalData()));
    }
}
