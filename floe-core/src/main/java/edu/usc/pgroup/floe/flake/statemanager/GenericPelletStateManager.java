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
     * Initialize the state manager
     *
     * @param args string encoded list of arguments.
     */
    @Override
    public void init(String args) {

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
    public final byte[] getIncrementalStateCheckpoint(String neighborId) {
        return new byte[0];
    }

    /**
     * Used to backup the states received from the neighbor flakes.
     *  @param nfid   flake id of the neighbor from which the state update is
     *               received.
     * @param checkpointdata the checkpoint data received from the
     *                       neighbor flake.
     */
    @Override
    public final void storeNeighborCheckpointToBackup(
            final String nfid,
            final byte[] checkpointdata) {
        //FIXME.. later. not requrired for the paper.
    }

    /**
     * Retrieve the state backed up for the given neighbor flake id.
     *
     * @param neighborFid neighbor's flake id.
     * @param keys      List of keys to be moved from the backup to the primary.
     * @return the backedup state assocuated with the given fid
     */
    @Override
    public final Map<String, PelletStateDelta> copyBackupToPrimary(
            final String neighborFid,
            final List<String> keys) {
        return null;
    }

    /**
     * Repartitions the state. Is used during state migrations for
     * loadbalance, scale in/out etc.
     *
     * @param selfFid      Flake id for the current flake.
     * @param neighborFids list of neighbor flake ids that hold the backup
     *                     for the current flake.
     * @return a map for neighbor/self fids to the list of state keys to be
     * transferred to that neighbor.
     * NOTE: THESE NEIGHBORS ARE ONLY THOSE WHO ALREADY HOLD THE "BACKUP",
     * the number of such neighbors depend on the "replication" factor.
     * Typically 1.
     */
    @Override
    public final Map<String, List<String>> repartitionState(
            final String selfFid,
            final List<String> neighborFids) {
        return null;
    }


}
