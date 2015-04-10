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

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.app.AppContext;
import edu.usc.pgroup.floe.app.Emitter;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.app.pellets.Pellet;
import edu.usc.pgroup.floe.app.pellets.PelletConfiguration;
import edu.usc.pgroup.floe.app.pellets.PelletContext;
import edu.usc.pgroup.floe.app.pellets.StateType;
import edu.usc.pgroup.floe.flake.FlakeToken;
import edu.usc.pgroup.floe.flake.FlakeUpdateListener;
import edu.usc.pgroup.floe.flake.statemanager.PelletState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author kumbhare
 */
public class WordCountReducer extends Pellet implements FlakeUpdateListener {

    /**
     * Key to be used to extract word from tuple.
     */
    private String tupleWordKey;

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PrintPellet.class);

    /**
     * The global metric registry that can be used by the pellet to track
     * application level metrics.
     */
    private Counter counter;

    /**
     * Pellet context.
     */
    private PelletContext pelletCtx;

    /**
     * Constructor.
     *
     * @param keyName name of the field from the input tuple to be
     *                used as the key for grouping tuples.
     */
    public WordCountReducer(final String keyName) {
        //super(keyName);
        tupleWordKey = keyName;
    }

    @Override
    public final void configure(final PelletConfiguration conf) {
        conf.setStateType(StateType.Reduce, tupleWordKey);
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
        MetricRegistry metricRegistry = pelletContext.getMetricRegistry();
        counter = metricRegistry.counter(MetricRegistry.name(
                WordCountReducer.class, "counter"));
        this.pelletCtx = pelletContext;
        this.pelletCtx.addFlakeUpdateListener(this);
        this.pelletCtx.startFlakeTracker();
        //use pelletContext.getPelletInstanceId() + "counter" if you need
        // pellet instance specific metric. Or just use "counter" if you want
        // flake level metrics.
    }

    /**
     * The teardown function, called when the topology is killed.
     * Or when the Pellet instance is scaled down.
     */
    @Override
    public final void teardown() {

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
     * Reducer specific execute function which is called for each input tuple
     * with the state corresponding to the key. This state will be persisted
     * across executes for each of the keys.
     *
     * @param t       input tuple received from the preceding pellet.
     * @param emitter An output emitter which may be used by the user to emmit.
     * @param state   State specific to the key value given in the tuple.
     */
    @Override
    public final void execute(final Tuple t,
                              final Emitter emitter,
                              final PelletState state) {
        if (t == null) {
            return;
        }

        /*Counter*/
        counter.inc();

        String word = (String) t.get(tupleWordKey);
        Integer count = 0;
        Object value = state.getValue("count");
        if (value != null) {
            count = (Integer) value + count;
        }
        count++;

        state.setValue("count", count);
        LOGGER.info("Count for {}: {}", word, count);
        LOGGER.info("Myfid: {}. Other Flakes: {}", pelletCtx.getFlakeId(),
                pelletCtx.getCurrentFlakeList());
        LOGGER.info("Counter Metric:{}", counter.getCount());
    }

    /**
     * This function is called exactly once when the initial flake list is
     * fetched.
     *
     * @param flakes list of currently initialized flakes.
     */
    @Override
    public final void initialFlakeList(final List<FlakeToken> flakes) {
        LOGGER.error("IN REDUER: {}", flakes);
    }

    /**
     * This function is called whenever a new flake is created for the
     * correspondong pellet.
     *
     * @param token flake token corresponding to the added flake.
     */
    @Override
    public final void flakeAdded(final FlakeToken token) {
        LOGGER.error("IN REDUER FL ADDED: {}", token);
    }

    /**
     * This function is called whenever a flake is removed for the
     * correspondong pellet.
     *
     * @param token flake token corresponding to the added flake.
     */
    @Override
    public final void flakeRemoved(final FlakeToken token) {

    }

    /**
     * This function is called whenever a data associated with a flake
     * corresponding to the given pellet is updated.
     *
     * @param token updated flake token.
     */
    @Override
    public final void flakeDataUpdated(final FlakeToken token) {

    }
}
