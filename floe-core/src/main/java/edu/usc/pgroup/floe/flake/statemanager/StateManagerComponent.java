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
import edu.usc.pgroup.floe.flake.FlakeComponent;
import org.zeromq.ZMQ;

import java.util.List;

/**
 * @author kumbhare
 */
public abstract class StateManagerComponent extends FlakeComponent {


    /**
     * Constructor.
     *
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     * @param port          Port to be used for sending checkpoint data.
     */
    public StateManagerComponent(final String flakeId,
                                 final String componentName,
                                 final ZMQ.Context ctx,
                                 final int port) {
        super(flakeId, componentName, ctx);
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
}
