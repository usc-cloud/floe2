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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import edu.usc.pgroup.floe.flake.statemanager.PelletStateDelta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

/**
 * @author kumbhare
 */
public class KryoStateSerializer implements StateSerializer {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(KryoStateSerializer.class);

    /**
     * Output stream.
     */
    private final ByteArrayOutputStream outstream;

    /**
     * Kryo serializer.
     */
    private Kryo kryo;

    /**
     * Kryo input.
     */
    private Input kryoInput;

    /**
     * Kryo output.
     */
    private Output kryoOutput;

    /**
     * Default kryo buffer size.
     */
    private static final int KRYO_BUFFER_SIZE = 1024;

    /**
     * Default kryo buffer size.
     */
    private static final int MAX_KRYO_BUFFER_SIZE = 1024 * 10; //10k

    /**
     * default constructor.
     */
    public KryoStateSerializer() {
        kryo = new Kryo();
        kryo.register(PelletStateDelta.class, 0);
        kryoInput = new Input();
        final int initialsize = 1024;
        outstream = new ByteArrayOutputStream(initialsize);
        kryoOutput = new Output(outstream, initialsize);
    }

    /**
     * To write the delta state to the output.
     * @param deltas list of pellet deltas to write to output.
     * @return serialized (aggregated) state.
     */
    @Override
    public final void writeDeltaState(final PelletStateDelta... deltas) {
        for (PelletStateDelta delta: deltas) {
            LOGGER.debug("Writing to kryoout");
            kryo.writeObject(kryoOutput, delta);
            LOGGER.debug("Current buffer: {}", kryoOutput.getBuffer());
        }
    }

    /**
     * Sets the serializer buffer to some value.
     *
     * @param buffer byte buffer containing number of pellet state deltas.
     */
    @Override
    public final void setBuffer(final byte[] buffer) {
        //kryoInput.rewind();
        //kryoInput.setBuffer(buffer);
        kryoInput = new Input(buffer);
    }

    /**
     * @return the buffer obtained from serializing the pellet deltas.
     */
    @Override
    public final byte[] getBuffer() {
        kryoOutput.flush();
        try {
            outstream.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }
        byte[] data = outstream.toByteArray();
        kryoOutput.clear();
        outstream.reset();
        return data;
    }

    /**
     * @return parses the state buffer and returns the next pellet state delta.
     */
    @Override
    public final PelletStateDelta getNextState() {
        return kryo.readObject(kryoInput, PelletStateDelta.class);
    }

    /**
     * Reset the serializer/deserializer buffer.
     */
    @Override
    public final void reset() {
        kryoInput.rewind();
        kryoOutput.clear();
    }
}
