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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.usc.pgroup.floe.app.Tuple;

/**
 * @author kumbhare
 */
public class KryoTupleSerializer implements TupleSerializer {

    /**
     * Default kryo serializer.
     */
    private Kryo kryo;

    /**
     * Kryo input (to be reused for various deserializations).
     */
    private Input kryoIn;

    /**
     * Kryo output (to be reused for various deserializations).
     */
    private Output kryoOut;

    /**
     * Default kryo buffer size.
     */
    private static final int KRYO_BUFFER_SIZE = 1000;

    /**
     * Max kryo buffer size.
     */
    private static final int MAX_KRYO_BUFFER_SIZE = 1000 * 10;

    /**
     * Default constructor.
     */
    public KryoTupleSerializer() {
        kryo = new Kryo();
        kryo.register(Tuple.class, 0);
        kryoOut = new Output(KRYO_BUFFER_SIZE, MAX_KRYO_BUFFER_SIZE);
        kryoIn = new Input();
    }

    /**
     * Serializes a tuple to bytes.
     *
     * @param tuple A message tuple.
     * @return serialized byte array.
     */
    @Override
    public final byte[] serialize(final Tuple tuple) {
        kryoOut.clear();
        kryo.writeObject(kryoOut, tuple);
        return kryoOut.getBuffer();
    }

    /**
     * Deserializes a byte array into a tuple object.
     *
     * @param serializedData serialized byte array obtained earlier from the
     *                       serialize function.
     * @return The deserialized Tuple object.
     */
    @Override
    public final Tuple deserialize(final byte[] serializedData) {
        kryoIn.rewind();
        kryoIn.setBuffer(serializedData);
        return kryo.readObject(kryoIn, Tuple.class);
    }
}
