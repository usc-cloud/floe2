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

package edu.usc.pgroup.floe.flake.messaging;

import edu.usc.pgroup.floe.app.Emitter;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.serialization.TupleSerializer;
import edu.usc.pgroup.floe.utils.Utils;
import org.zeromq.ZMQ;

import java.util.List;

/**
 * @author kumbhare
 */
public class MessageEmitter implements Emitter {

    /**
     * flake's id to which this emitter belongs.
     */
    private final String flakeId;

    /**
     * ZMQ Context.
     */
    private final ZMQ.Context zcontex;


    /**
     * ZMQ PUSH socket to send data to the backend.
     */
    private final ZMQ.Socket socket;

    /**
     * Tuple serializer.
     */
    private final TupleSerializer serializer;

    /**
     * Constructor.
     * @param context shared ZMQ context to be used in inproc comm. for
     *                      receiving message from the flake.
     * @param fid flake's id to which this pellet belongs.
     * @param tupleSerializer custom tuple serializer.
     */
    public MessageEmitter(final String fid, final ZMQ.Context context,
                          final TupleSerializer tupleSerializer) {
        this.flakeId = fid;
        this.zcontex = context;
        this.socket = context.socket(ZMQ.PUSH);
        this.socket.connect(Utils.Constants.FLAKE_SENDER_FRONTEND_SOCK_PREFIX
                + flakeId);
        this.serializer = tupleSerializer;
    }

    /**
     * To emmit a set of tuples on the output port.
     *
     * @param messages a list of messages.
     */
    @Override
    public final void emit(final List<Tuple> messages) {
        for (Tuple m: messages) {
            emit(m);
        }
    }

    /**
     * To emmit a single tuples on the output port.
     *
     * @param message a message.
     */
    @Override
    public final void emit(final Tuple message) {
        emit(Utils.Constants.DEFAULT_STREAM_NAME, message);
    }

    /**
     * To emmit a single tuple to the given output stream.
     * @param streamName name of the stream
     * @param message a tuple message to emit
     */
    public final void emit(final String streamName, final Tuple message) {
        byte[] serialized = serializer.serialize(message);
        this.socket.sendMore(streamName);
        this.socket.send(serialized, 0);
    }
}
