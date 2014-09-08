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

import edu.usc.pgroup.floe.app.BasePellet;
import edu.usc.pgroup.floe.app.Emitter;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kumbhare
 */
public class WordMultiStreamPellet extends BasePellet {
    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(WordMultiStreamPellet.class);

    /**
     * First stream name.
     */
    private final String stream1;

    /**
     * Second stream name.
     */
    private final String stream2;

    /**
     * list of words to emmit on first stream.
     */
    private final String[] firstWords;

    /**
     * list of words to emmit on second stream.
     */
    private final String[] secondWords;


    /**
     * Constructor.
     * @param stream1Name First stream name.
     * @param stream2Name Second stream name.
     * @param firstStreamWords list of words to emmit on first stream.
     * @param secondStreamWords list of words to emmit on second stream.
     */
    public WordMultiStreamPellet(final String stream1Name,
                                 final String stream2Name,
                                 final String[] firstStreamWords,
                                 final String[] secondStreamWords) {
        this.stream1 = stream1Name;
        this.stream2 = stream2Name;
        this.firstWords = firstStreamWords;
        this.secondWords = secondStreamWords;
    }

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
        LOGGER.info("Executing multi stream word pellet.");
        int i = 0, j = 0, k = 0;
        while (true) {

            Tuple ot = null;
            if (i == 0) {
                if (j == firstWords.length) {
                    j = 0;
                }
                ot = new Tuple();
                ot.put("word", firstWords[j++]);
                emitter.emit(stream1, ot);
                i = 1;
            } else if (i == 1) {
                if (k == secondWords.length) {
                    k = 0;
                }
                ot = new Tuple();
                ot.put("word", secondWords[k++]);
                emitter.emit(stream2, ot);
                i = 0;
            }
            try {
                Thread.sleep(Utils.Constants.MILLI);
            } catch (InterruptedException e) {
                LOGGER.error("Exception: {}", e);
                break;
            }
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
        ArrayList<String> outputStreams = new ArrayList<String>();
        outputStreams.add("boys");
        outputStreams.add("girls");
        return outputStreams;
    }
}
