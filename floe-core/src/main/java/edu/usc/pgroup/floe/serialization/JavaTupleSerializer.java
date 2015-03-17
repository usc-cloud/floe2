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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * @author kumbhare
 */
public class JavaTupleSerializer implements TupleSerializer {
    /**
     * Serializes a tuple to bytes.
     *
     * @param tuple A message tuple.
     * @return serialized byte array.
     */
    @Override
    public final byte[] serialize(final Tuple tuple) {
        try {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(tuple);
            oos.close();
            return bos.toByteArray();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
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
        Tuple ret = null;
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(serializedData);
            ObjectInputStream ois = new ObjectInputStream(bis);
            ret = (Tuple) ois.readObject();
            ois.close();
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return ret;
    }
}
