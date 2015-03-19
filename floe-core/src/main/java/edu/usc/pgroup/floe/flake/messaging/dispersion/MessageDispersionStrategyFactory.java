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

import edu.usc.pgroup.floe.thriftgen.TChannel;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kumbhare
 */
public final class MessageDispersionStrategyFactory {


    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(MessageDispersionStrategyFactory.class);

    /**
     * Hiding default constructor.
     */
    private MessageDispersionStrategyFactory() {

    }

    /**
     * Factory function for creating the MessageDispersionStrategy.
     * @param appName Application name.
     * @param destPelletName dest pellet name to be used to get data from ZK.
     * @param channel type of the channel (edge) in the application.
     * @return new instance of MessageDispersionStrategy based on the edge type.
     * @throws java.lang.ClassNotFoundException if the given channel type is
     * invalid or the class for custom strategy is not found.
     */
    public static MessageDispersionStrategy
            getMessageDispersionStrategy(
                final String destPelletName,
                final String appName,
                final TChannel channel) throws ClassNotFoundException {

        MessageDispersionStrategy strategy
                = (MessageDispersionStrategy) Utils.instantiateObject(
                channel.get_dispersionClass());

//        switch (channel.get_channelType()) {
//            case ROUND_ROBIN:
//                strategy = new RRDispersionStrategy();
//                break;
//            case REDUCE:
//                strategy = new ElasticReducerDispersion();
//                break;
//            case LOAD_BALANCED:
//            case CUSTOM:
//            default:
//                throw new ClassNotFoundException(channel.toString());
//        }
        if (strategy != null) {
            strategy.initialize(
                    appName, destPelletName, channel.get_channelArgs());
        }
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

    /**
     * Factory function for creating the MessageDispersionStrategy.
     * @param metricRegistry Metrics registry used to log various metrics.
     * @param context shared ZMQ context.
     * @param channel channel type.
     * @param flakeId Current flake id.
     * @return returns the associated local dispersion strategy.
     * @throws java.lang.ClassNotFoundException if the channel type is invalid
     *
    public static FlakeLocalDispersionStrategy
                    getFlakeLocalDispersionStrategy(
            final MetricRegistry metricRegistry,
            final ZMQ.Context context,
            final TChannel channel, final String flakeId)
            throws ClassNotFoundException {*/

    /**
     * Factory function for creating the Local Message DispersionStrategy.
     * @param channel channel type associated with the given dataflow edge.
     * @return returns the associated local dispersion strategy.
     */
    public static FlakeLocalDispersionStrategy
        getFlakeLocalDispersionStrategy(final TChannel channel) {

        FlakeLocalDispersionStrategy strategy = null;

        strategy = (FlakeLocalDispersionStrategy) Utils.instantiateObject(
                channel.get_localDispersionClass());

        if (strategy != null) {
            strategy.initialize(channel.get_channelArgs());
        }
        return strategy;
    }
}
