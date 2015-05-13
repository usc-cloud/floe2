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

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.app.AppContext;
import edu.usc.pgroup.floe.app.Emitter;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.app.pellets.PelletConfiguration;
import edu.usc.pgroup.floe.app.pellets.PelletContext;
import edu.usc.pgroup.floe.app.pellets.Signallable;
import edu.usc.pgroup.floe.app.pellets.StatelessPellet;
import edu.usc.pgroup.floe.signals.PelletSignal;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author kumbhare
 */
public class WordPellet extends StatelessPellet implements Signallable {
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
     * Metrics registry.
     */
    private Meter tweetRateMeter;

    /**
     * DEFAULT LOOP MAX value.
     */
    private static final long DEFAULT_LOOPMAX = (long) Math.pow(10, 10);

    /**
     * loop max for busy wait.
     */
    private long loopMax;

    /**
     * Constructor.
     * @param w List of words to emmit in a loop.
     * @param sleepTime sleeptime interval between two words.
     */
    public WordPellet(final String[] w,
                      final long sleepTime) {
        this.words = w;
        this.interval = sleepTime;
        this.loopMax = DEFAULT_LOOPMAX;
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
    public final void onStart(final AppContext appContext,
                      final PelletContext pelletContext) {
        MetricRegistry metrics = pelletContext.getMetricRegistry();
        tweetRateMeter = metrics.meter(
                MetricRegistry.name(WordPellet.class, "MyWordGen"));
    }

    /**
     * Should use the conf object to configure different pellet configuration.
     * @param conf pellet configuration.
     */
    @Override
    protected final void configureStateLessPellet(
            final PelletConfiguration conf) {
        conf.markAsSourcePellet();
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


        int i = (int) (Math.random() * words.length);

        Tuple ot = new Tuple();
        ot.put("word", words[i]);
        LOGGER.debug("Emmitting: {}", ot);
        emitter.emit(ot);

        tweetRateMeter.mark();

        //busy wait.
        long j = 0;
        while (j < loopMax) {
            j++;
        }
//
//
//        //Meter tweetRateMeter = metrics.meter(MetricRegistry.name
//        // ("MyWordGen"));
//
//        while (true) {
//            if (i == words.length) {
//                i = 0;
//            }
//
//            try {
//                Thread.sleep(interval);
//            } catch (InterruptedException e) {
//                LOGGER.error("Exception: {}", e);
//                break;
//            }
//            i++;
//            //tweetRateMeter.mark();
//        }
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

    /**
     * Called when a signal is received for the component.
     *
     * @param signal the signal received for this pellet.
     */
    @Override
    public final void onSignal(final PelletSignal signal) {
        String lmstr = (String) Utils.deserialize(signal.getSignalData());
        loopMax = Integer.parseInt(lmstr);
    }
}
