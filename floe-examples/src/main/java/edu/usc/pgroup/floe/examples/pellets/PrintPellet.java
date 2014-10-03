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
import edu.usc.pgroup.floe.app.StatefulPellet;
import edu.usc.pgroup.floe.app.PelletContext;
import edu.usc.pgroup.floe.app.Signallable;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.flake.statemanager.PelletState;
import edu.usc.pgroup.floe.signals.PelletSignal;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author kumbhare
 */
public class PrintPellet extends StatefulPellet implements Signallable {

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
     * The setup function is called once to let the pellet initialize.
     * @param appContext Application's context. Some data related to
     *                   application's deployment.
     * @param pelletContext Pellet instance context. Related to this
     *                      particular pellet instance.
     */
    @Override
    public final void setup(final AppContext appContext,
                      final PelletContext pelletContext) {
        this.peInstanceId = pelletContext.getPelletInstanceId();
    }

    /**
     * The onStart function is called once just before executing the pellet
     * and after the setup function. Typically, this is used by a data source
     * pellet which does not depend on external data source but generates
     * tuples on its own.
     *
     * @param emitter An ouput emitter which may be used by the user to emmit
     *                results.
     */
    @Override
    public void onStart(final Emitter emitter) {

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
     */
    @Override
    public final void execute(
            final Tuple t,
            final Emitter emitter,
            final PelletState state) {
        if (t == null) {
            LOGGER.info("Dummy execute PRINT.");
        } else {
            Object value = state.getValue("count");
            Integer count = 0;
            if (value != null) {
                count = (Integer) value + 1;
            }
            state.setValue("count", count);
            LOGGER.info("{} : {} : Received: {}",
                    peInstanceId,
                    count,
                    t.get("word"));
            emitter.emit(t);
        }
    }

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
     * Called when a signal is received for the component.
     *
     * @param signal the signal received for this pellet.
     */
    @Override
    public final void onSignal(final PelletSignal signal) {
        LOGGER.info("RECEIVED SIGNAL: {}",
                Utils.deserialize(signal.getSignalData()));
    }
}
