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

import java.util.List;

/**
 * The main Emitter interface for Pellets to send out message tuples.
 *
 * @author kumbhare
 */
public interface Emitter {
    /**
     * To emmit a set of tuples on the output port.
     *
     * @param messages a list of messages.
     */
    void emit(List<Tuple> messages);

    /**
     * To emmit a single tuples on the output port.
     *
     * @param message a message.
     */
    void emit(Tuple message);
}
