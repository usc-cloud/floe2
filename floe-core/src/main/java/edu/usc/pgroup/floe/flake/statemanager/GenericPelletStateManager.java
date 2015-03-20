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

import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.app.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kumbhare
 */
public class GenericPelletStateManager implements StateManager {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(GenericPelletStateManager.class);

    /**
     * The pellet instance id to pellet state map.
     */
    private ConcurrentHashMap<String, PelletState> pelletStateMap;

    /**
     * Default Constructor.
     */
    public GenericPelletStateManager() {
        pelletStateMap = new ConcurrentHashMap<>(); //fixme. add size to
        // optimize
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
    public final synchronized PelletState getState(final String peId,
                                                   final Tuple tuple) {
        if (!pelletStateMap.containsKey(peId)) {
            LOGGER.info("Creating new state for peid: {}", peId);
            pelletStateMap.put(peId, new PelletState(peId));
        }
        return pelletStateMap.get(peId);
    }

    /**
     * Returns the object (state) associated with the given local pe instance.
     * The tuple may be used to further divide the state (e.g. in case of
     * reducer pellet, the tuple's key will be used to divide the state).
     *
     * @param peId Pellet's instance id.
     * @param key  The value associated with the correspnding field name (this
     *             is used during recovery since we do not have access to the
     *             entire tuple, but just the key).
     * @return pellet state corresponding to the given peId and key value
     * combination.
     */
    @Override
    public final PelletState getState(final String peId, final String key) {
        return null;
    }

    /**
     * Checkpoint state and return the serialized delta to send to the backup
     * nodes.
     *
     * @return serialized delta to send to the backup nodes.
     */
    @Override
    public final byte[] getIncrementalStateCheckpoint() {
        return new byte[0];
    }

    /**
     * Used to backup the states received from the neighbor flakes.
     *  @param nfid   flake id of the neighbor from which the state update is
     *               received.
     * @param deltas a list of pellet state deltas received from the flake.
     */
    @Override
    public final void storeBackupState(final String nfid,
                                       final List<PelletStateDelta> deltas) {
        //FIXME.. later. not requrired for the paper.
    }

    /**
     * Get the state backed up for the given neighbor flake id.
     *
     * @param neighborFid neighbor's flake id.
     * @return the backedup state assocuated with the given fid
     */
    @Override
    public final Map<String, PelletStateDelta>
                    retrieveBackupState(final String neighborFid) {
        //FIXME.. later. not required for the paper.
        return null;
    }

    /**
     * Starts the msg recovery process for the given neighbor.
     *
     * @param nfid flake id of the neighbor for which the msg recovery is to
     *             start.
     */
    @Override
    public void startMsgRecovery(final String nfid) {
        //FIXEME.. later.. not required for the paper.
    }
}
