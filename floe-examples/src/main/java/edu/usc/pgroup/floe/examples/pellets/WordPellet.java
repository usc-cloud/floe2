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

import edu.usc.pgroup.floe.app.AppContext;
import edu.usc.pgroup.floe.app.Emitter;
import edu.usc.pgroup.floe.app.StatelessPellet;
import edu.usc.pgroup.floe.app.PelletContext;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author kumbhare
 */
public class WordPellet extends StatelessPellet {
    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(WordPellet.class);

    /**
     * List of words to emmit in a loop.
     */
    private final String[] words;

    /**
     * Interval between emitting words.
     */
    private final long interval;

    /**
     * Constructor.
     * @param w List of words to emmit in a loop.
     * @param sleepTime sleeptime interval between two words.
     */
    public WordPellet(final String[] w,
                      final long sleepTime) {
        this.words = w;
        this.interval = sleepTime;
    }

    /**
     * Constructor.
     * @param w List of words to emmit in a loop.
     */
    public WordPellet(final String[] w) {
        this(w, Utils.Constants.MILLI);
    }

    /**
     * The setup function is called once to let the pellet initialize.
     * @param appContext Application's context. Some data related to
     *                   application's deployment.
     * @param pelletContext Pellet instance context. Related to this
     *                      particular pellet instance.
     */
    @Override
    public void setup(final AppContext appContext,
                      final PelletContext pelletContext) {

    }

    /**
     * The onStart function is called once just before executing the pellet
     * and after the setup function. Typically, this is used by a data source
     * pellet which does not depend on external data source but generates
     * tuples on its own.
     *
     * @param emitter An output emitter which may be used by the user to emmit
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
        LOGGER.info("Executing word pellet.");
        int i = 0;
        while (true) {
            if (i == words.length) {
                i = 0;
            }
            Tuple ot = new Tuple();
            ot.put("word", words[i]);
            emitter.emit(ot);
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                LOGGER.error("Exception: {}", e);
                break;
            }
            i++;
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
     * @return The names of the streams to be used later during emitting
     * messages.
     */
    @Override
    public final List<String> getOutputStreamNames() {
        return null;
    }
}
