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

package edu.usc.pgroup.floe.serialization;

import edu.usc.pgroup.floe.app.Tuple;

/**
 * Tuple serializer interface.
 * @author kumbhare
 */
public interface TupleSerializer {
    /**
     * Serializes a tuple to bytes.
     * @param tuple A message tuple.
     * @return serialized byte array.
     */
    byte[] serialize(Tuple tuple);

    /**
     * Deserializes a byte array into a tuple object.
     * @param serializedData serialized byte array obtained earlier from the
     *                       serialize function.
     * @return The deserialized Tuple object.
     */
    Tuple deserialize(byte[] serializedData);
}
