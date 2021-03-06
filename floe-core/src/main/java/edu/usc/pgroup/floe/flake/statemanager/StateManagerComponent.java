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
import edu.usc.pgroup.floe.flake.FlakeComponent;
import org.zeromq.ZMQ;

import java.util.List;

/**
 * @author kumbhare
 */
public abstract class StateManagerComponent extends FlakeComponent {


    /**
     * Constructor.
     * @param metricRegistry Metrics registry used to log various metrics.
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     * @param port          Port to be used for sending checkpoint data.
     */
    public StateManagerComponent(final MetricRegistry metricRegistry,
                                 final String flakeId,
                                 final String componentName,
                                 final ZMQ.Context ctx,
                                 final int port) {
        super(metricRegistry, flakeId, componentName, ctx);
    }

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
    public abstract PelletState getState(final String peId,
                                         final Tuple tuple);

    /**
     * Returns the object (state) associated with the given local pe instance.
     * The tuple may be used to further divide the state (e.g. in case of
     * reducer pellet, the tuple's key will be used to divide the state).
     * @param peId Pellet's instance id.
     * @param key The value associated with the correspnding field name (this
     *            is used during recovery since we do not have access to the
     *            entire tuple, but just the key).
     * @return pellet state corresponding to the given peId and key value
     * combination.
     */
    public abstract PelletState getState(final String peId,
                                         final String key);

    /**
     * Checkpoint state and return the serialized delta to send to the backup
     * nodes.
     * @return serialized delta to send to the backup nodes.
     */
    public abstract byte[] checkpointState();

    /**
     * Used to backup the states received from the neighbor flakes.
     * @param nfid flake id of the neighbor from which the state update is
     *             received.
     * @param deltas a list of pellet state deltas received from the flake.
     */
    public abstract void backupState(final String nfid,
                                     final List<PelletStateDelta> deltas);

    /**
     * Get the state backed up for the given neighbor flake id.
     * @param neighborFid neighbor's flake id.
     * @return the backedup state assocuated with the given fid
     */
    public abstract java.util.Map<String, PelletStateDelta> getBackupState(
            final String neighborFid);

    /**
     * Starts the msg recovery process for the given neighbor.
     * @param nfid flake id of the neighbor for which the msg recovery is to
     *             start.
     */
    public abstract void startMsgRecovery(final String nfid);
}
