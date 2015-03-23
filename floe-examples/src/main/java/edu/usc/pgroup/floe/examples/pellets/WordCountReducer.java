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
import edu.usc.pgroup.floe.app.pellets.Pellet;
import edu.usc.pgroup.floe.app.pellets.PelletConfiguration;
import edu.usc.pgroup.floe.app.pellets.PelletContext;
import edu.usc.pgroup.floe.app.pellets.StateType;
import edu.usc.pgroup.floe.flake.statemanager.PelletState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author kumbhare
 */
public class WordCountReducer extends Pellet {

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
        //super(keyName);
        tupleWordKey = keyName;
    }

    @Override
    public final void configure(final PelletConfiguration conf) {
        conf.setStateType(StateType.LocalOnly);
    }

    /**
     * The setup function is called once to let the pellet initialize.
     * @param appContext Application's context. Some data related to
     *                   application's deployment.
     * @param pelletContext Pellet instance context. Related to this
     *                      particular pellet instance.
     */
    @Override
    public void onStart(final AppContext appContext,
                      final PelletContext pelletContext) {

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
        if (t == null) {
            return;
        }
        String word = (String) t.get(tupleWordKey);
        Integer count = 0;
        Object value = state.getValue("count");
        if (value != null) {
            count = (Integer) value + count;
        }
        count++;
        if (count == 1 && word.equals("the")) {
            LOGGER.error("I have 'the'");
        }
        state.setValue("count", count);
        LOGGER.info("Count for {}: {}", word, count);
    }
}
