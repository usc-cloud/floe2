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

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import edu.usc.pgroup.floe.app.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.io.ByteArrayOutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kumbhare
 */
public class ReducerStateManager extends StateManagerComponent
        implements PelletStateUpdateListener {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ReducerStateManager.class);

    /**
     * Key field name used by the reducer for grouping.
     */
    private final String keyFieldName;


    /**
     * The pellet instance id to pellet state map. PelletState map is a map
     * from the custom key identifier to the pellet state.
     */
    private ConcurrentHashMap<String,
            HashMap<Object, PelletState>> pelletStateMap;

    /**
     * State checkpointing component to periodically perform delta
     * checkpointing.
     */
    private final StateCheckpointComponent checkpointer;

    /**
     * component to backup state as well as messages.
     */
    private final ReducerStateBackupComponent backupComponent;

    /**
     * Constructor.
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     * @param fieldName     The fieldName used by the reducer for grouping.
     * @param port          Port to be used for sending checkpoint data.
     */
    public ReducerStateManager(final String flakeId,
                               final String componentName,
                               final ZMQ.Context ctx,
                               final String fieldName,
                               final int port) {
        super(flakeId, componentName, ctx, port);
        this.pelletStateMap = new ConcurrentHashMap<>(); //fixme. add size,
        this.keyFieldName = fieldName;
        checkpointer = new StateCheckpointComponent(flakeId,
                componentName + "-CHECKPOINTER", ctx, this, port);
        backupComponent = new ReducerStateBackupComponent(flakeId,
                componentName + "-STBACKUP", ctx, fieldName);
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

        if (tuple == null) {
            return null;
        }

        synchronized (pelletStateMap) {
            if (!pelletStateMap.containsKey(peId)) {
                LOGGER.info("Creating new state for peid: {}", peId);
                pelletStateMap.put(peId, new HashMap<Object, PelletState>());
            }
        }
        HashMap<Object, PelletState> keyStateMap = pelletStateMap.get(peId);

        String value = (String) tuple.get(keyFieldName);
        if (!keyStateMap.containsKey(value)) {
            LOGGER.info("Creating new state for value: {}", value);
            keyStateMap.put(value, new PelletState(peId, value, this));
        }

        return keyStateMap.get(value);
    }

    /**
     * Checkpoint state and return the serialized delta to send to the backup
     * nodes.
     *
     * @return serialized delta to send to the backup nodes.
     */
    @Override
    public final byte[] checkpointState() {
        //FIXME: MAKE THIS INTO A FACTORY.
        //KryoStateSerializer serializer = new KryoStateSerializer();

        Kryo kryo = new Kryo();
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        Output kryoOut = new Output(outStream);

        synchronized (pelletStateMap) {
            for (HashMap<Object, PelletState> keyToPstateMap
                    : pelletStateMap.values()) {
                for (PelletState pState: keyToPstateMap.values()) {
                    LOGGER.debug("starting checkpointing:{}",
                            pState.getCustomId());
                    PelletStateDelta delta = pState.startDeltaCheckpointing();
                    LOGGER.debug("to checkpoint this:{}",
                            delta.getDeltaState());
                    if (delta.getDeltaState().size() > 0) {
                        //serializer.writeDeltaState(delta);
                        kryo.writeObject(kryoOut, delta);
                    }
                    pState.finishDeltaCheckpointing();
                }
            }
        }

        kryoOut.flush();
        kryoOut.close();
        return outStream.toByteArray();
        //byte[] serialized = serializer.getBuffer();
        //LOGGER.info("Serialied size:{}", serialized.length);
        //return serialized;
    }

    /**
     * Used to backup the states received from the neighbor flakes.
     *
     * @param nfid   flake id of the neighbor from which the state update is
     *               received.
     * @param deltas a list of pellet state deltas received from the flake.
     */
    @Override
    public final void backupState(final String nfid,
                            final List<PelletStateDelta> deltas) {
        backupComponent.backupState(nfid, deltas);
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

        checkpointer.startAndWait();
        backupComponent.startAndWait();
        notifyStarted(true);

        terminateSignalReceiver.recv();
        backupComponent.stopAndWait();
        checkpointer.stopAndWait();
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
                                   final Object customId,
                                   final String key,
                                   final Object value) {
        /*LOGGER.info("State updated for: {}, reducer key:{} , state key:{}, "
                        + "new value:{}", srcPeId, customId, key, value);*/
    }


}
