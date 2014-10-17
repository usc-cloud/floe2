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

package edu.usc.pgroup.floe.flake.messaging.dispersion;

import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.flake.messaging
        .dispersion.elasticreducer.ElasticReducerDispersion;
import edu.usc.pgroup.floe.flake.messaging
        .dispersion.elasticreducer.ElasticReducerFlakeLocalDispersion;
import edu.usc.pgroup.floe.thriftgen.TChannelType;
import org.zeromq.ZMQ;

/**
 * @author kumbhare
 */
public final class MessageDispersionStrategyFactory {

    /**
     * Hiding default constructor.
     */
    private MessageDispersionStrategyFactory() {

    }

    /**
     * Factory function for creating the MessageDispersionStrategy.
     * @param appName Application name.
     * @param destPelletName dest pellet name to be used to get data from ZK.
     * @param channelType type of the channel (edge) in the application.
     * @param args Any arguments to be sent to the Strategy Class while
     *             initialization.
     * @return new instance of MessageDispersionStrategy based on the edge type.
     * @throws java.lang.ClassNotFoundException if the given channel type is
     * invalid or the class for custom strategy is not found.
     */
    public static MessageDispersionStrategy
            getMessageDispersionStrategy(
                final String destPelletName,
                final String appName,
                final TChannelType channelType,
                final String args) throws ClassNotFoundException {

        MessageDispersionStrategy strategy = null;

        switch (channelType) {
            case ROUND_ROBIN:
                strategy = new RRDispersionStrategy();
                break;
            case REDUCE:
                strategy = new ElasticReducerDispersion();
                break;
            case LOAD_BALANCED:
            case CUSTOM:
            default:
                throw new ClassNotFoundException(channelType.toString());
        }

        strategy.initialize(appName, destPelletName, args);
        return strategy;
    }

    /**
     * Factory function for creating the MessageDispersionStrategy.
     * @param metricRegistry Metrics registry used to log various metrics.
     * @param context shared ZMQ context.
     * @param flakeId Current flake id.
     * param args Any arguments to be sent to the Strategy Class while
     *             initialization.
     * @return new instance of MessageDispersionStrategy based on the edge type.
     * @throws java.lang.ClassNotFoundException if the given channel type is
     * invalid or the class for custom strategy is not found.
     *
    public static FlakeLocalDispersionStrategy
    getFlakeLocalDispersionStrategy(
            final MetricRegistry metricRegistry,
            final TChannelType channelType,
            final ZMQ.Context context,
            final String flakeId,
            final String args) throws ClassNotFoundException {

        FlakeLocalDispersionStrategy strategy = null;

        switch (channelType) {
            case ROUND_ROBIN:
                strategy = new RRFlakeLocalDispersionStrategy(
                        metricRegistry, srcPelletName,
                        context, flakeId);
                break;
            case REDUCE:
                strategy = new ElasticReducerFlakeLocalDispersion(
                        metricRegistry, srcPelletName,
                        context, flakeId);
                break;
            case LOAD_BALANCED:
            case CUSTOM:
            default:
                throw new ClassNotFoundException(channelType.toString());
        }

        strategy.initialize(args);
        return strategy;
    }*/

    public static FlakeLocalDispersionStrategy
                    getFlakeLocalDispersionStrategy(
            final MetricRegistry metricRegistry,
            final ZMQ.Context context,
            final String flakeId) {
        FlakeLocalDispersionStrategy strat
                = new ElasticReducerFlakeLocalDispersion(metricRegistry, context,
                flakeId);
        strat.initialize(null);
        return strat;
    }
}
