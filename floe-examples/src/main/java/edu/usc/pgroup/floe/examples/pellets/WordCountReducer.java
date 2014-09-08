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

import edu.usc.pgroup.floe.app.Emitter;
import edu.usc.pgroup.floe.app.ReducerPellet;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.app.signals.PelletState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kumbhare
 */
public class WordCountReducer extends ReducerPellet {

    /**
     * Key to be used to extract word from tuple.
     */
    private String tupleWordKey;

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PrintPellet.class);

    /**
     * Constructor.
     *
     * @param keyName name of the field from the input tuple to be
     *                used as the key for grouping tuples.
     */
    public WordCountReducer(final String keyName) {
        super(keyName);
        tupleWordKey = keyName;
    }

    /**
     * Used to initialize the state for a given key.
     *
     * @param key The unique key for which the state should be initialized.
     * @return the newly initialized state.
     */
    @Override
    public final PelletState initializeState(final Object key) {
        PelletState state = new PelletState();
        state.setState(new Integer(0));
        return state;
    }

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
    public final void execute(final Tuple t,
                        final Emitter emitter,
                        final PelletState state) {
        String word = (String) t.get(tupleWordKey);
        Integer count = (Integer) state.getState();
        count++;
        state.setState(count);
        LOGGER.info("Count for {}: {}", word, count);
    }

    /**
     * The setup function is called once to let the pellet initialize.
     */
    @Override
    public final void setup() {

    }

    /**
     * The onStart function is called once just before executing the pellet
     * and after the setup function. Typically, this is used by a data source
     * pellet which does not depend on external data source but generates
     * tuples on its own.
     *
     * @param emitter An output emitter which may be used by the user to emmit
     *                results.
     */
    @Override
    public final void onStart(final Emitter emitter) {

    }

    /**
     * The teardown function, called when the topology is killed.
     * Or when the Pellet instance is scaled down.
     */
    @Override
    public final void teardown() {

    }
}
