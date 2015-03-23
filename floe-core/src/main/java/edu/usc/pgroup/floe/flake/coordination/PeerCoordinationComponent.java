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

package edu.usc.pgroup.floe.flake.coordination;

import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.flake.FlakeComponent;
import org.zeromq.ZMQ;

/**
 * @author kumbhare
 */
public abstract class PeerCoordinationComponent extends FlakeComponent {

    /**
     * the application name.
     */
    private final String appName;

    /**
     * pellet's name to which this flake belongs.
     */
    private final String pelletName;


    /**
     * Constructor.
     * @param metricRegistry Metrics registry used to log various metrics.
     * @param app the application name.
     * @param pellet pellet's name to which this flake belongs.
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     */
    public PeerCoordinationComponent(final MetricRegistry metricRegistry,
                                     final String app,
                                     final String pellet,
                                     final String flakeId,
                                     final String componentName,
                                     final ZMQ.Context ctx) {
        super(metricRegistry, flakeId, componentName, ctx);
        this.appName = app;
        this.pelletName = pellet;
    }

    /**
     * @return the application name.
     */
    public final String getAppName() {
        return appName;
    }

    /**
     * @return the pellet name
     */
    public final String getPelletName() {
        return pelletName;
    }
}
