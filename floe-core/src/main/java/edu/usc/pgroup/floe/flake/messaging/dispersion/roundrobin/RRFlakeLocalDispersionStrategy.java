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

package edu.usc.pgroup.floe.flake.messaging.dispersion.roundrobin;

import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.flake.messaging
        .dispersion.FlakeLocalDispersionStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kumbhare
 */
public class RRFlakeLocalDispersionStrategy
        extends FlakeLocalDispersionStrategy {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RRFlakeLocalDispersionStrategy.class);

    /**
     * Current index in the RR strategy.
     */
    private int currentIndex;


    /**
     * List of target pellet instances.
     */
    private List<String> targetPelletInstances;

    /**
     * Default constructor.
     */
    public RRFlakeLocalDispersionStrategy() {

    }
    /**
     * Constructor.
     * @param metricRegistry Metrics registry used to log various metrics.
     * @param context shared ZMQ context.
     * @param flakeId Current flake id.
     *
    public RRFlakeLocalDispersionStrategy(
                final MetricRegistry metricRegistry,
                final ZMQ.Context context,
                final String flakeId) {
        super(metricRegistry, context, flakeId);
    }*/

    /**
     * Initializes the strategy.
     *
     * @param args the arguments sent by the user. Fix Me: make this a better
     *             interface.
     */
    @Override
    public final void initialize(final String args) {
        targetPelletInstances = new ArrayList<>();
        currentIndex = 0;
    }


    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     *
     * @return the list of target instances to send the given tuple.
     */
    public final String getTargetPelletInstances() {
        if (currentIndex >= targetPelletInstances.size()) {
            currentIndex = 0;
        }

        if (targetPelletInstances.size() == 0) {
            return null;
        }

        /*List<String> target = targetPelletInstances.subList(
                currentIndex,
                currentIndex + 1);*/
        String peid = targetPelletInstances.get(currentIndex);
        currentIndex++;
        return peid;
    }

    /*@Override
    public final void sendToPellets(final ZMQ.Socket from, final ZMQ.Socket
            to) {
        //do something.
        String peinstanceid = getTargetPelletInstances();

        if (peinstanceid != null) {
            to.sendMore(peinstanceid);
            Utils.forwardCompleteMessage(from, to);
        } else {
            Utils.recvAndignore(from);
        }
    }*/

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     *
     * @param tuple tuple object.
     * @param args  custom arguments sent by the source flake with the tuple.
     * @return the list of target instances to send the given tuple.
     */
    @Override
    public final String getTargetPelletInstance(
            final Tuple tuple,
            final List<String> args) {
        return getTargetPelletInstances();
    }

    /**
     * @return the current backchannel data (e.g. for loadbalancing or the
     * token on the ring etc.)
     *
    @Override
    public final byte[] getCurrentBackchannelData() {
        byte[] b = new byte[]{'a'};
        LOGGER.info("Sending data on bk channel {}", b);
        return b;
    }*/

    /**
     * Called whenever a new pellet is added.
     *
     * @param pelletId pellet instance id which has been added.
     */
    @Override
    public final void pelletAdded(final String pelletId) {
        if (!targetPelletInstances.contains(pelletId)) {
            targetPelletInstances.add(pelletId);
        }
    }

    /**
     * Called whenever a pellet is removed.
     *
     * @param pelletId pellet instance id which has been added.
     */
    @Override
    public final void pelletRemoved(final String pelletId) {
        if (targetPelletInstances.contains(pelletId)) {
            targetPelletInstances.remove(pelletId);
        }
    }
}
