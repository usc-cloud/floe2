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

import java.util.List;
import java.util.Map;

/**
 * @author kumbhare
 */
public interface StateManager {

    /**
     * Returns the object (state) associated with the given local pe instance.
     * The tuple may be used to further divide the state (e.g. in case of
     * reducer pellet, the tuple's key will be used to divide the state).
     * @param peId Pellet's instance id.
     * @param tuple The tuple object which may be used to further divide the
     *              state based on some criterion so that state
     *              transfers/checkpointing may be improved.
     * @return pellet state corresponding to the given peId and tuple
     * combination.
     */
    PelletState getState(final String peId,
                                         final Tuple tuple);

    /**
     * Returns the object (state) associated with the given local pe instance.
     * The tuple may be used to further divide the state (e.g. in case of
     * reducer pellet, the State's key will be used to divide the state).
     * NOTE: This is a state specific key, AND NOT NECESSARILY SAME AS THE
     * MESSAGE KEY.
     * @param peId Pellet's instance id.
     * @param key The value associated with the corresponding field name (this
     *            is used during recovery since we do not have access to the
     *            entire tuple, but just the key).
     * @return pellet state corresponding to the given peId and key value
     * combination.
     */
    PelletState getState(final String peId,
                                         final String key);

    /**
     * Checkpoint state and return the serialized delta to send to the backup
     * nodes.
     * @return serialized delta to send to the backup nodes.
     */
    byte[] getIncrementalStateCheckpoint();

    /**
     * Used to backup the states received from the neighbor flakes.
     * @param nfid flake id of the neighbor from which the state update is
     *             received.
     * @param checkpointdata the checkpoint data received from the
     *                       neighbor flake.
     */
    void storeNeighborCheckpointToBackup(
            final String nfid,
            final byte[] checkpointdata);

    /**
     * Retrieve the state backed up for the given neighbor flake id.
     * @param neighborFid neighbor's flake id.
     * @param keys List of keys to be moved from the backup to the primary.
     * @return the backedup state assocuated with the given fid
     */
    Map<String, PelletStateDelta> copyBackupToPrimary(
            final String neighborFid, List<String> keys);


    /**
     * Repartitions the state. Is used during state migrations for
     * loadbalance, scale in/out etc.
     * @param selfFid Flake id for the current flake.
     * @param neighborFids list of neighbor flake ids that hold the backup
     *                     for the current flake.
     * @return a map for neighbor/self fids to the list of state keys to be
     * transferred to that neighbor.
     * NOTE: THESE NEIGHBORS ARE ONLY THOSE WHO ALREADY HOLD THE "BACKUP",
     * the number of such neighbors depend on the "replication" factor.
     * Typically 1.
     */
    Map<String, List<String>> repartitionState(String selfFid,
            List<String> neighborFids);
}
