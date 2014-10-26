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

package edu.usc.pgroup.floe.flake.messaging.sender;

import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.flake.FlakeComponent;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * @author kumbhare
 */
public class SenderMEComponent extends FlakeComponent {


    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SenderMEComponent.class);

    /**
     * Constructor.
     * @param metricRegistry Metrics registry used to log various metrics.
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     */
    public SenderMEComponent(final MetricRegistry metricRegistry,
                             final String flakeId,
                             final String componentName,
                             final ZMQ.Context ctx) {
        super(metricRegistry, flakeId, componentName, ctx);
    }

    /**
     * Starts all the sub parts of the given component and notifies when
     * components starts completely. This will be in a different thread,
     * so no need to worry.. block as much as you want.
     *
     * @param terminateSignalReceiver terminate signal receiver.
     */
    @Override
    protected final void runComponent(
            final ZMQ.Socket terminateSignalReceiver) {

        final ZMQ.Socket frontend  = getContext().socket(ZMQ.PULL);
        frontend.bind(
                Utils.Constants.FLAKE_SENDER_FRONTEND_SOCK_PREFIX
                        + getFid());


        final ZMQ.Socket middleend  = getContext().socket(ZMQ.PUB);
        middleend.bind(
                Utils.Constants.FLAKE_SENDER_MIDDLEEND_SOCK_PREFIX
                        + getFid());



        ZMQ.Poller pollerItems = new ZMQ.Poller(2);
        pollerItems.register(frontend, ZMQ.Poller.POLLIN);
        pollerItems.register(terminateSignalReceiver, ZMQ.Poller.POLLIN);

        notifyStarted(true);
        while (!Thread.currentThread().isInterrupted()) {
            pollerItems.poll();
            if (pollerItems.pollin(0)) {
                Utils.forwardCompleteMessage(frontend, middleend);
            } else if (pollerItems.pollin(1)) {
                LOGGER.warn("Terminating flake sender ME: {}", getFid());
                terminateSignalReceiver.recv();
                break;
            }
        }
        LOGGER.warn("Closing flake middleend sockets");
        frontend.close();
        middleend.close();
        notifyStopped(true);
    }
}
