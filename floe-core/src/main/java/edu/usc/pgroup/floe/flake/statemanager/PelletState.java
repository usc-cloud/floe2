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

package edu.usc.pgroup.floe.flake.statemanager;

import java.util.HashMap;

/**
 * @author kumbhare
 */
public class PelletState {

    /**
     * pellet instance id to which this pellet state belongs.
     */
    private final String peId;

    /**
     * A custom identifier that can be used to further identify this state's
     * owner.
     */
    private final String customId;

    /**
     * Pellet specific state object.
     */
    private HashMap<String, Object> pelletState;

    /**
     * The listener object which receives notification when a state is updated.
     */
    private PelletStateUpdateListener updateListener;


    /**
     * Constructor.
     * @param peInstanceId pellet instance id to which this pellet state
     *                     belongs.
     * @param customSubId A custom identifier that can be used to further
     *                 identify this state's owner.
     * @param listener The listener object which receives notification when a
     *                 state is updated.
     */
    PelletState(final String peInstanceId,
                final String customSubId,
                final PelletStateUpdateListener listener) {
        this.pelletState = new HashMap<>();
        this.updateListener = listener;
        this.peId = peInstanceId;
        this.customId = customSubId;
    }

    /**
     * Constructor.
     * @param peInstanceId pellet instance id to which this pellet state
     *                     belongs.
     * @param listener The listener object which receives notification when a
     *                 state is updated.
     */
    PelletState(final String peInstanceId,
                final PelletStateUpdateListener listener) {
        this(peInstanceId, null, listener);
    }

    /**
     * sets the state for the pellet.
     * @param key the key/name for the update.
     * @param state the new state object.
     */
    public final void setValue(final String key, final Object state) {
        this.pelletState.put(key, state);
        if (updateListener != null) {
            updateListener.stateUpdated(peId, customId, key, state);
        }
    }

    /**
     * @param key the key/name to retrieve.
     * @return the current state of the pellet.
     */
    public final Object getValue(final String key) {
        return pelletState.get(key);
    }
}
