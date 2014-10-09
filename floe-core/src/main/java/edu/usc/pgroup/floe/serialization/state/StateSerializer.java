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

package edu.usc.pgroup.floe.serialization.state;


import edu.usc.pgroup.floe.flake.statemanager.PelletStateDelta;

/**
 * @author kumbhare
 */
public interface StateSerializer {
    /**
     * To write the delta state to the output.
     * @param deltas list of pellet deltas to write to output.
     * @return serialized (aggregated) state.
     */
    void writeDeltaState(PelletStateDelta... deltas);


    /**
     * Sets the serializer buffer to some value.
     * @param buffer byte buffer containing number of pellet state deltas.
     */
    void setBuffer(byte[] buffer);

    /**
     * @return the buffer obtained from serializing the pellet deltas.
     */
    byte[] getBuffer();

    /**
     * @return parses the state buffer and returns the next pellet state delta.
     */
    PelletStateDelta getNextState();

    /**
     * Reset the serializer/deserializer buffer.
     */
    void reset();
}
