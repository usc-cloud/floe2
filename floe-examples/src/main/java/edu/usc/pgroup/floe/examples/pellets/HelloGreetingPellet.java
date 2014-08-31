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

package edu.usc.pgroup.floe.examples.pellets;

import edu.usc.pgroup.floe.app.Emitter;
import edu.usc.pgroup.floe.app.SignallablePellet;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.app.signals.Signal;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kumbhare
 */
public class HelloGreetingPellet implements SignallablePellet {


    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(HelloGreetingPellet.class);

    /**
     * The setup function is called once to let the pellet initialize.
     */
    @Override
    public void setup() {

    }

    /**
     * The onStart function is called once just before executing the pellet
     * and after the setup function. Typically, this is used by a data source
     * pellet which does not depend on external data source but generates
     * tuples on its own.
     *
     * @param emitter An ouput emitter which may be used by the user to emmit
     *                results.
     */
    @Override
    public void onStart(final Emitter emitter) {

    }


    /**
     * The execute method which is called for each tuple.
     *
     * @param t       input tuple received from the preceding pellet.
     * @param emitter An output emitter which may be used by the user to emmit
     */
    @Override
    public final void execute(final Tuple t, final Emitter emitter) {
        if (t == null) {
            LOGGER.info("Dummy execute PRINT.");
        } else {
            LOGGER.info("Hello " + t.getDummy());
        }
    }

    /**
     * The teardown function, called when the topology is killed.
     * Or when the Pellet instance is scaled down.
     */
    @Override
    public void teardown() {

    }

    /**
     * Called when a signal is received for the component.
     *
     * @param signal the signal received for this pellet.
     */
    @Override
    public final void onSignal(final Signal signal) {
        LOGGER.info("RECEIVED SIGNAL: {}",
                Utils.deserialize(signal.getSignalData()));
    }
}
