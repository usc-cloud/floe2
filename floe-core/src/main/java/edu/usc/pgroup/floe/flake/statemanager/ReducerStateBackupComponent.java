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
import edu.usc.pgroup.floe.serialization.SerializerFactory;
import edu.usc.pgroup.floe.serialization.TupleSerializer;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author kumbhare
 */
public class ReducerStateBackupComponent extends FlakeComponent {


    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ReducerStateBackupComponent.class);


    /**
     * Message backup.
     * Map from "neighbor's" flake id to a time sorted map of messages.
     * Neighbor Fid -> Reducer Key -> SortedTS -> Tuple
     */
    private final Map<String,
            Map<String, SortedMap<Long, Tuple>>> messageBackup;


    /**
     * State backup.
     * Neighbor Fid -> Reducer Key -> Merged State.
     */
    private final Map<String,
            Map<String, PelletStateDelta>> stateBackup;


    /**
     * Serializer to be used to serialize and deserialized the data tuples.
     */
    private final TupleSerializer tupleSerializer;

    /**
     * Key field name used by the reducer for grouping.
     */
    private final String keyFieldName;

    /**
     * Constructor.
     *  @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     * @param fieldName     The fieldName used by the reducer for grouping.
     */
    public ReducerStateBackupComponent(final String flakeId,
                                       final String componentName,
                                       final ZMQ.Context ctx,
                                       final String fieldName) {
        super(flakeId, componentName, ctx);
        this.messageBackup = new HashMap<>();
        this.tupleSerializer = SerializerFactory.getSerializer();
        this.keyFieldName = fieldName;
        this.stateBackup = new HashMap<>();
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
        ZMQ.Socket backupListener = getContext().socket(ZMQ.PULL);
        backupListener.bind(Utils.Constants.FLAKE_MSG_BACKUP_PREFIX + getFid());

        ZMQ.Poller pollerItems = new ZMQ.Poller(2);
        pollerItems.register(terminateSignalReceiver, ZMQ.Poller.POLLIN);
        pollerItems.register(backupListener, ZMQ.Poller.POLLIN);

        notifyStarted(true);

        while (!Thread.currentThread().isInterrupted()) {
            pollerItems.poll();
            if (pollerItems.pollin(0)) {
                //terminate.
                LOGGER.warn("Terminating state backup component");
                terminateSignalReceiver.recv();
                break;
            } else {
                String nfid = backupListener.recvStr(Charset.defaultCharset());
                byte[] btuple = backupListener.recv();

                Tuple t = tupleSerializer.deserialize(btuple);

                addTupleToBackup(nfid, t);


            }
        }
        backupListener.close();
        notifyStopped(true);
    }

    /**
     * Adds the tuple to the backup.
     * @param nfid neighbor's flake id.
     * @param t the tuple to add.
     */
    private void addTupleToBackup(final String nfid,
                                  final Tuple t) {

        Map<String, SortedMap<Long, Tuple>> keyMap = messageBackup.get(nfid);

        if (keyMap == null) {
            keyMap = new HashMap<>();
            messageBackup.put(nfid, keyMap);
        }


        //get key value from tuple.
        //FIXME: COULD BE OTHER THAN STRING.
        String keyValue = (String) t.get(keyFieldName);

        SortedMap<Long, Tuple> messages = keyMap.get(keyValue);

        if (messages == null) {
            messages = Collections.synchronizedSortedMap(
                    new TreeMap<Long, Tuple>(Collections.reverseOrder()));
            keyMap.put(keyValue, messages);
        }


        Long ts = (Long) t.get(Utils.Constants.SYSTEM_TS_FIELD_NAME);
        LOGGER.info("Backing up msg: {}", t);
        synchronized (messages) {
            messages.put(ts, t);
        }
    }

    /**
     * Backs up (merges) the state update received from neighbor flake with
     * the currently backed up state.
     * @param nfid neighbor's flake id.
     * @param deltas a list of pellet state deltas received from the flake.
     */
    public final void backupState(final String nfid,
                            final List<PelletStateDelta> deltas) {

        //Merge with the currently backed up state.
        Map<String, PelletStateDelta> keyStateMap = stateBackup.get(nfid);


        Map<String, SortedMap<Long, Tuple>> keyMsgMap = messageBackup.get(nfid);


        if (keyStateMap == null) {
            keyStateMap = new HashMap<>();
            stateBackup.put(nfid, keyStateMap);
        }


        for (PelletStateDelta delta: deltas) {
            String key = delta.getKey();
            Long ts = delta.getTimestamp();

            PelletStateDelta pelletState = keyStateMap.get(key);
            if (pelletState == null) {
                keyStateMap.put(key, delta);
            } else {
                pelletState.mergeDelta(delta);
            }


            //Remove the tuples from the msg backup that are no longer required.
            if (keyMsgMap != null) { //otherwise no msgs to remove. ignore.
                SortedMap<Long, Tuple> messages = keyMsgMap.get(key);
                if (messages != null) { //otherwise ignore.

                    //FIXME: SUBTRACT THE 2 *
                    //FIXME: MAX_LATENCY VALUE FROM TIME STAMP HERE
                    synchronized (messages) {
                        SortedMap<Long, Tuple> msgsToRemove
                                = messages.tailMap(ts);
                        LOGGER.info("Clearing msgs {} from backup for "
                                        + "fid/key: {}.{} before ts: {}",
                                          msgsToRemove, nfid, key, ts);
                        msgsToRemove.clear();
                    }
                }
            }
        }
    }
}