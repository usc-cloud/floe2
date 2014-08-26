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

import java.io.Serializable;


/**
 * The Pellet interface which is the basic unit of user code.
 *
 * @author kumbhare
 */
public interface Pellet extends Serializable {

    /**
     * The setup function is called once to let the pellet initialize.
     * TODO: ADD A FLOE CONTEXT OBJECT.
     */
    void setup();


    /**
     * The onStart function is called once just before executing the pellet
     * and after the setup function. Typically, this is used by a data source
     * pellet which does not depend on external data source but generates
     * tuples on its own.
     * @param emitter An output emitter which may be used by the user to emmit
     *                results.
     */
    void onStart(Emitter emitter);

    /**
     * The execute method which is called for each tuple.
     *
     * @param t       input tuple received from the preceding pellet.
     * @param emitter An output emitter which may be used by the user to emmit
     *                results.
     */
    void execute(Tuple t, Emitter emitter);

    /**
     * The teardown function, called when the topology is killed.
     * Or when the Pellet instance is scaled down.
     */
    void teardown();

    /**
     * Dummy fucntion to test deserialization.
     * @return a dummyvalue
     */
    String getDummy();
}
