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

package edu.usc.pgroup.floe.app.pellets;

import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.flake.FlakeToken;
import edu.usc.pgroup.floe.flake.FlakeUpdateListener;
import edu.usc.pgroup.floe.flake.FlakesTracker;

import java.util.List;

/**
 * @author kumbhare
 */
public class PelletContext {

    /**
     * Pellet's instance id.
     */
    private final String pelletInstanceId;

    /**
     * pellet's name (as specified during application building).
     */
    private final String pelletName;

    /**
     * flake id to which this pellet belongs.
     */
    private final String flakeId;


    /**
     * Path cache to monitor the tokens.
     */
    private FlakesTracker flakesTracker;

    /**
     * The global metric registry that can be used by the pellet to track
     * application level metrics.
     */
    private final MetricRegistry metricRegistry;

    /**
     * Constructor.
     * @param appName application's name.
     * @param peInstanceId Pellet's instance id.
     * @param peName pellet's name (as specified during application building)
     * @param fid flake id to which this pellet belongs.
     * @param registry The global metric registry that can be used by the
     */
    public PelletContext(final String appName,
                         final String peInstanceId,
                         final String peName,
                         final String fid,
                         final MetricRegistry registry) {
        this.pelletInstanceId = peInstanceId;
        this.pelletName = peName;
        this.flakeId = fid;
        this.metricRegistry = registry;
        this.flakesTracker = new FlakesTracker(appName, peName);
        this.flakesTracker.start();
    }

    /**
     * @return the pellet's instance id.
     */
    public final String getPelletInstanceId() {
        return pelletInstanceId;
    }

    /**
     * @return The global metric registry that can be used by the pellet to
     * track application level metrics.
     */
    public final MetricRegistry getMetricRegistry() { return metricRegistry; }

    /**
     * @return pellet's name (as specified during application building)
     */
    public final String getPelletName() { return pelletName; }

    /**
     * @return flake id to which this pellet belongs.
     */
    public final String getFlakeId() { return flakeId; }

    /**
     * @return a list of flakes running across the cluster corresponding to
     * this pellet type.
     */
    public final List<FlakeToken> getCurrentFlakeList() {
        return flakesTracker.getCurrentFlakes();
    }

    /**
     * Adds the flake update listener to the flake tracker.
     * @param listener FlakeUpdateListener instance.
     */
    public final void addFlakeUpdateListener(
            final FlakeUpdateListener listener) {
        flakesTracker.addFlakeUpdateListener(listener);
    }
}
