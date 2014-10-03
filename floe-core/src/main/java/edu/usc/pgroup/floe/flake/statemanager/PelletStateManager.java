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

package edu.usc.pgroup.floe.flake.statemanager;

import edu.usc.pgroup.floe.app.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kumbhare
 */
public class PelletStateManager extends StateManagerComponent
        implements PelletStateUpdateListener {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PelletStateManager.class);

    /**
     * The pellet instance id to pellet state map.
     */
    private ConcurrentHashMap<String, PelletState> pelletStateMap;

    /**
     * Constructor.
     *
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     */
    public PelletStateManager(final String flakeId,
                               final String componentName,
                               final ZMQ.Context ctx) {
        super(flakeId, componentName, ctx);
        pelletStateMap = new ConcurrentHashMap<>(); //fixme. add size,
        // loadfactor
    }

    /**
     * Returns the object (state) associated with the given local pe instance.
     * The tuple may be used to further divide the state (e.g. in case of
     * reducer pellet, the tuple's key will be used to divide the state).
     *
     * @param peId  Pellet's instance id.
     * @param tuple The tuple object which may be used to further divide the
     *              state based on some criterion so that state
     *              transfers/checkpointing may be improved.
     * @return pellet state corresponding to the given peId and tuple
     * combination.
     */
    @Override
    public final PelletState getState(final String peId, final Tuple tuple) {
        if (!pelletStateMap.containsKey(peId)) {
            LOGGER.info("Creating new state for peid: {}", peId);
            pelletStateMap.put(peId, new PelletState(peId, this));
        }
        return pelletStateMap.get(peId);
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
        notifyStarted(true);
        terminateSignalReceiver.recv();
        notifyStopped(true);
    }

    /**
     * @param srcPeId  pellet instance id which resulted in this update
     * @param customId A custom identifier that can be used to further
     *                 identify this state's owner.
     * @param key      the key for the state update.
     * @param value    the updated value.
     * NOTE: THIS HAS TO BE THREAD SAFE....
     */
    @Override
    public final void stateUpdated(final String srcPeId,
                             final String customId,
                             final String key,
                             final Object value) {
        LOGGER.info("State updated for: {}, key:{}, new value:{}",
                srcPeId, key, value);
    }
}
