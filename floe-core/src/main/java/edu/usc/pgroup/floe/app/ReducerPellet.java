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

import java.util.HashMap;
import java.util.Map;

/**
 * @author kumbhare
 */
public abstract class ReducerPellet extends BasePellet {

    /**
     * Key field name to be used for grouping tuples.
     */
    private final String keyFieldName;


    /**
     * Map from unique key values to the user defined state.
     */
    private final Map<Object, PelletState> keyValueStateMap;

    /**
     * Constructor.
     * @param keyName name of the field from the input tuple to be
     *                     used as the key for grouping tuples.
     */
    public ReducerPellet(final String keyName) {
        this.keyFieldName = keyName;
        keyValueStateMap = new HashMap<>();
    }

    /**
     * @return The field name which is to be used for grouping tuples.
     */
    public final String getKeyFieldName() {
        return keyFieldName;
    }

    /**
     * The execute method which is called for each tuple.
     *
     * @param t       input tuple received from the preceding pellet.
     * @param emitter An output emitter which may be used by the user to emmit
     */
    @Override
    public final void execute(final Tuple t, final Emitter emitter) {
        PelletState state;
        if (t == null) {
            return;
        }
        Object key = t.get(keyFieldName);
        if (keyValueStateMap.containsKey(key)) {
            state = keyValueStateMap.get(key);
        } else {
            state = initializeState(key);
            keyValueStateMap.put(key, state);
        }

        execute(t, emitter, state);
    }

    /**
     * Used to initialize the state for a given key.
     * @param key The unique key for which the state should be initialized.
     * @return the newly initialized state.
     */
    public abstract PelletState initializeState(Object key);

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
