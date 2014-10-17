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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

/**
 * @author kumbhare
 */
public class PelletStateDelta /*implements KryoSerializable*/ {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PelletStateDelta.class);

    /**
     * Timestamp for the latest update.
     */
    private Long timestamp;
    /**
     * The key associated with this state.
     */
    private String key;
    /**
     * the delta state since the last checkpoint.
     */
    private HashMap<String, Object> deltaState;

    /**
     * Constructor.
     * @param ts The Timestamp for the latest update.
     * @param k The key associated with this state.
     * @param delState the delta state since the last checkpoint.
     */
    public PelletStateDelta(final Long ts,
                     final String k,
                     final HashMap<String, Object> delState) {
        this.timestamp = ts;
        this.key = k;
        this.deltaState = delState;
    }

    /**
     *
     */
    public PelletStateDelta() {
        this.deltaState = new HashMap<>();
    }

    /**
     * @return timestamp for the latest update.
     */
    public final Long getTimestamp() {
        return timestamp;
    }

    /**
     * @return The key associated with this state.
     */
    public final String getKey() {
        return key;
    }

    /**
     * @return the delta state since the last checkpoint.
     */
    public final HashMap<String, Object> getDeltaState() {
        return deltaState;
    }

    /**
     * Used during backup. Merges the delta received from the neighbor flake
     * with this one.
     * @param delta delta state received from the neighbor flake.
     */
    public final void mergeDelta(final PelletStateDelta delta) {
        this.timestamp = delta.getTimestamp();
        //key should be same.
        assert (key.equalsIgnoreCase(delta.getKey()));

        this.deltaState.putAll(delta.getDeltaState());
    }

    /**
     * Clear the state from backup.
     */
    public void clear() {
        deltaState.clear();
    }

    /*@Override
    public final void write(
            final Kryo kryo,
            final Output output) {
        LOGGER.info("Serializing using kryo");
        output.writeLong(timestamp);
        output.writeString(key);
        kryo.writeObject(output, deltaState);
    }

    @Override
    public final void read(final Kryo kryo,
                     final Input input) {
        timestamp = input.readLong();
        key = input.readString();
        deltaState = kryo.readObject(input, HashMap.class);
    }*/
}
