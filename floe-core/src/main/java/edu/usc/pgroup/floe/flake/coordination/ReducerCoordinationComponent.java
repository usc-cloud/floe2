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

package edu.usc.pgroup.floe.flake.coordination;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import edu.usc.pgroup.floe.container.FlakeControlCommand;
import edu.usc.pgroup.floe.flake.FlakeToken;
import edu.usc.pgroup.floe.flake.ZKFlakeTokenCache;
import edu.usc.pgroup.floe.flake.statemanager.PelletState;
import edu.usc.pgroup.floe.flake.statemanager.PelletStateDelta;
import edu.usc.pgroup.floe.flake.statemanager.StateManagerComponent;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import edu.usc.pgroup.floe.zookeeper.zkcache.PathChildrenUpdateListener;
import org.apache.curator.framework.recipes.cache.ChildData;

import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

/**
 * @author kumbhare
 */
public class ReducerCoordinationComponent extends CoordinationComponent
        implements PathChildrenUpdateListener {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ReducerCoordinationComponent.class);

    /**
     * level of tolerance. (i.e. number of flake
     *                       failures to tolerate).
     */
    private final Integer toleranceLevel;

    /**
     * Fixme. Should not be string.
     * K = toleranceLevel Neighbor flakes in counter clockwise direction.
     *
    private final SortedMap<Integer, String> stateBackupNeighbors;*/


    /**
     * Fixme. Should not be string.
     * K = toleranceLevel Neighbor flakes in counter clockwise direction.
     */
    private SortedMap<Integer, String> neighborsToBackupMsgsFor;

    /**
     * Token and connect information for neighbors to backup for.
     */
    private final Map<String, FlakeToken> flakeToDataPortMap;


    /**
     * Flake's current token on the ring.
     */
    private final Integer myToken;

    /**
     * State manager instance.
     */
    private final StateManagerComponent stateManager;

    /**
     * Path cache to monitor the tokens.
     */
    private ZKFlakeTokenCache flakeCache;

    /**
     * The control socket to get a ping wheenver the flakes are removed from
     * the token ring.
     */
    private final String tokenListenerSockPrefix = "inproc://flake-token-";

    /**
     * Constructor.
     *
     * @param app           the application name.
     * @param pellet        pellet's name to which this flake belongs.
     * @param flakeId       Flake's id to which this component belongs.
     * @param token       This flake's current token value.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     * @param tolerance level of tolerance. (i.e. number of flake
     *                       failures to tolerate).
     * @param stManager State manager associated with this flake.
     */
    public ReducerCoordinationComponent(final String app,
                                        final String pellet,
                                        final String flakeId,
                                        final Integer token,
                                        final String componentName,
                                        final ZMQ.Context ctx,
                                        final Integer tolerance,
                                        final StateManagerComponent stManager) {
        super(app, pellet, flakeId, componentName, ctx);
        this.toleranceLevel = tolerance;
        //this.stateBackupNeighbors = new TreeMap<>();
        this.neighborsToBackupMsgsFor = new TreeMap<>(
                Collections.reverseOrder()); //Reverse order compared to how
                // message hash is found. FIXME: ADD A PROPOER DOCUMENTATION
                // HERE.
        this.myToken = token;
        this.flakeToDataPortMap = new HashMap<>();
        this.stateManager = stManager;
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
        String pelletTokenPath = ZKUtils.getApplicationPelletTokenPath(
                getAppName(), getPelletName());

        flakeCache = new ZKFlakeTokenCache(pelletTokenPath, this);

        boolean success = true;

        ZMQ.Socket stateSoc = getContext().socket(ZMQ.SUB);

        try {
            //flakeCache.start();
            flakeCache.rebuild();


            List<ChildData> childData = flakeCache.getCurrentCachedData();

            //extractNeighboursToBackupState(childData);
            neighborsToBackupMsgsFor =
                    extractNeighboursToSubscribeForMessages(childData);

            updateStateSubscriptions(stateSoc, neighborsToBackupMsgsFor, null);

        } catch (Exception e) {
            LOGGER.error("Could not start token monitor.{}", e);
            success = false;
        }


        ZMQ.Socket controlSoc = getContext().socket(ZMQ.PULL);
        controlSoc.bind(tokenListenerSockPrefix + getFid());

        final ZMQ.Socket msgReceivercontrolForwardSocket
                = getContext().socket(ZMQ.REQ);
        msgReceivercontrolForwardSocket.connect(
                Utils.Constants.FLAKE_RECEIVER_CONTROL_FWD_PREFIX
                        + getFid());

        success = true;

        notifyStarted(success);

        final int numPollSocks = 3;
        ZMQ.Poller pollerItems = new ZMQ.Poller(numPollSocks);
        pollerItems.register(terminateSignalReceiver, ZMQ.Poller.POLLIN);
        pollerItems.register(stateSoc, ZMQ.Poller.POLLIN);
        pollerItems.register(controlSoc, ZMQ.Poller.POLLIN);

        final int checkpointDelay = 5000; //check the 'failure' detection
        // logic here.

        while (!Thread.currentThread().isInterrupted()) {
            //LOGGER.info("Receiving checkpointed State");
            int polled = pollerItems.poll(checkpointDelay);
            if (pollerItems.pollin(0)) {
                //terminate.
                LOGGER.warn("Terminating state checkpointing");
                terminateSignalReceiver.recv();
                break;
            } else if (pollerItems.pollin(1)) {
                //Merge with the state manager.
                String nfid = stateSoc.recvStr(Charset.defaultCharset());
                String last = stateSoc.recvStr(Charset.defaultCharset());

                Boolean scalingDown = Boolean.parseBoolean(last);

                if (scalingDown) {
                    LOGGER.info("Scaling down flake: {}", nfid);
                }

                LOGGER.debug("State delta received from:{}", nfid);
                byte[] serializedState = stateSoc.recv();

                List<PelletStateDelta> deltas
                        = extractPelletStateDeltas(serializedState);

                stateManager.backupState(nfid, deltas);

                if (scalingDown) {
                    LOGGER.info("Scaling down NEIGHBOR flake: {}", nfid);
                    initiateScaleDownAndTakeOver(nfid);
                }

                //stateManager.backupState(nfid, deltas);
            } else if (pollerItems.pollin(2)) {
                try {
                    controlSoc.recv(); //receive and ignore. otherwise the
                    // pollin will go into infinite loop.

                    LOGGER.info("UPDATING STATE SUBSCRIPTIONS.");
                    List<ChildData> childData
                            = flakeCache.getCurrentCachedData();

                    //extractNeighboursToBackupState(childData);
                    SortedMap<Integer, String> currentNeighbors
                           = extractNeighboursToSubscribeForMessages(childData);

                    LOGGER.info("Current neighbors:{}.", currentNeighbors);

                    SortedMap<Integer, String> newFlakes
                            = getNewlyAddedFlakes(currentNeighbors,
                                                    neighborsToBackupMsgsFor);

                    LOGGER.info("Newly added neighbors:{}.", newFlakes);

                    SortedMap<Integer, String> removedFlakes
                            = getRemovedFlakes(currentNeighbors,
                            neighborsToBackupMsgsFor);

                    LOGGER.info("Removed neighbors:{}.", removedFlakes);


                    neighborsToBackupMsgsFor.putAll(newFlakes);

                    for (Integer key: removedFlakes.keySet()) {
                        neighborsToBackupMsgsFor.remove(key);
                    }

                    updateStateSubscriptions(stateSoc,
                            newFlakes, removedFlakes);




                    List<String> neighbors = new ArrayList<String>(
                            neighborsToBackupMsgsFor.values());

                    FlakeControlCommand newCommand = new FlakeControlCommand(
                            FlakeControlCommand.Command.UPDATE_SUBSCRIPTION,
                            neighbors
                    );
                    msgReceivercontrolForwardSocket.send(
                            Utils.serialize(newCommand), 0);
                    msgReceivercontrolForwardSocket.recv();


                } catch (Exception e) {
                    LOGGER.error("Could not start token monitor.{}", e);
                    success = false;
                }
            }
        }


        stateSoc.close();
        controlSoc.close();
        msgReceivercontrolForwardSocket.close();

        notifyStopped(success);
    }

    /**
     * Gets the newly added neighbors.
     * @param currentNeighbors current neighbors obtained from ZK.
     * @param oldNeighbors old neighbors.
     * @return the newly added neighbors.
     */
    private SortedMap<Integer, String> getNewlyAddedFlakes(
            final SortedMap<Integer, String> currentNeighbors,
            final SortedMap<Integer, String> oldNeighbors) {
        SortedMap<Integer, String> added = new TreeMap<>();

        for (Map.Entry<Integer, String> current
                : currentNeighbors.entrySet()) {
            if (oldNeighbors.containsKey(current.getKey())) {
                continue;
            }
            added.put(current.getKey(), current.getValue());
        }
        return added;
    }


    /**
     * Gets the removed neighbors.
     * @param currentNeighbors current neighbors obtained from ZK.
     * @param oldNeighbors old neighbors.
     * @return the removed neighbors.
     */
    private SortedMap<Integer, String> getRemovedFlakes(
            final SortedMap<Integer, String> currentNeighbors,
            final SortedMap<Integer, String> oldNeighbors) {
        SortedMap<Integer, String> removed = new TreeMap<>();

        for (Map.Entry<Integer, String> old
                : oldNeighbors.entrySet()) {
            if (currentNeighbors.containsKey(old.getKey())) {
                continue;
            }
            removed.put(old.getKey(), old.getValue());
        }
        return removed;
    }

    /**
     * Update state subscriptions.
     * @param stateSoc the socket that should connect and subscribe.
     * @param neighborsToAdd newNeighbors to subscribe and connect to.
     * @param neighborsToRemove old neighbors to remove from the subscription.
     */
    private void updateStateSubscriptions(
            final ZMQ.Socket stateSoc,
            final SortedMap<Integer, String> neighborsToAdd,
            final SortedMap<Integer, String> neighborsToRemove) {
        /**
         * ZMQ socket connection publish the state to the backups.
         */
        if (neighborsToAdd != null) {
            for (Map.Entry<Integer, String> finfo
                    : neighborsToAdd.entrySet()) {

                FlakeToken connctInfo
                        = flakeToDataPortMap.get(finfo.getValue());

                String ssConnetStr
                        = Utils.Constants.FLAKE_STATE_SUB_SOCK_PREFIX
                        + connctInfo.getIpOrHost() + ":"
                        + connctInfo.getStateCheckptPort();

                stateSoc.subscribe(finfo.getValue().getBytes());

                LOGGER.info("connecting STATE CHECKPOINTER "
                        + "to listen for state updates: {}", ssConnetStr);

                stateSoc.connect(ssConnetStr);
            }
        }

        if (neighborsToRemove != null) {
            for (Map.Entry<Integer, String> finfo
                    : neighborsToRemove.entrySet()) {

                FlakeToken connctInfo
                        = flakeToDataPortMap.get(finfo.getValue());

                String ssConnetStr
                        = Utils.Constants.FLAKE_STATE_SUB_SOCK_PREFIX
                        + connctInfo.getIpOrHost() + ":"
                        + connctInfo.getStateCheckptPort();

                stateSoc.unsubscribe(finfo.getValue().getBytes());

                LOGGER.info("DISCONNECTING STATE CHECKPOINTER "
                        + "to listen for state updates: {}", ssConnetStr);

                stateSoc.disconnect(ssConnetStr);
            }
        }
    }

    /**
     * Initiates the scale down process for the flake and take's over it's
     * state and key space.
     * @param nfid neighbor's flake id which is to be scaled down.
     */
    private void initiateScaleDownAndTakeOver(final String nfid) {
        new RecoveryHandler(nfid).start();
    }

    /**
     * Deserializes the incoming state information and extracts a list of
     * pelletstatedeltas from it.
     * @param checkpointdata serialized checkpoint data recieved from
     *                       neighbor flake.
     * @return list of pellet state deltas.
     */
    private List<PelletStateDelta> extractPelletStateDeltas(
            final byte[] checkpointdata) {

        List<PelletStateDelta> deltas = new ArrayList<>();
        if (checkpointdata != null && checkpointdata.length > 0) {
            try {
                Kryo kryo = new Kryo();
                Input kryoIn = new Input(checkpointdata);

                //stateSerializer.setBuffer(checkpointdata);
                //PelletStateDelta pes = stateSerializer.getNextState();

                while (!kryoIn.eof()) {
                    PelletStateDelta pes
                            = kryo.readObject(kryoIn,
                                                PelletStateDelta.class);
                    LOGGER.debug("received:{}",
                            pes.getDeltaState());
                    deltas.add(pes);
                }
                LOGGER.debug("Finished deserialization.");
                kryoIn.close();
            } catch (Exception e) {
                LOGGER.warn("Exception: {}", e);
            }
        } else {
            LOGGER.debug("No new updates to checkpoint.");
        }

        return deltas;
    }

    /**
     * Finds k neighbor flakes in counter clockwise direction.
     * @param childData data received from ZK cache.
     *
    private void extractNeighboursToBackupState(
            final List<ChildData> childData) {
        SortedMap<Integer, String> allFlakes = new TreeMap<>(
                Collections.reverseOrder());

        for (ChildData child: childData) {
            String path = child.getPath();
            Integer data = (Integer) Utils.deserialize(child.getData());
            allFlakes.put(data, path);
            LOGGER.info("CHILDREN: {} , TOKEN: {}", path, data);
        }

        SortedMap<Integer, String> tail = allFlakes.tailMap(myToken);
        Iterator<Integer> iterator = tail.keySet().iterator();
        iterator.next(); //ignore the self's token.


        int i = 0;
        for (; i < toleranceLevel && iterator.hasNext(); i++) {
            Integer neighborToken = iterator.next();
            stateBackupNeighbors.put(neighborToken,
                    allFlakes.get(neighborToken));
        }

        Iterator<Integer> headIterator = allFlakes.keySet().iterator();
        for (; i < toleranceLevel && headIterator.hasNext(); i++) {
            Integer neighborToken = headIterator.next();
            stateBackupNeighbors.put(neighborToken,
                    allFlakes.get(neighborToken));
        }

        LOGGER.info("ME:{}, I WILL BACKUP MY STATE AT: {}", myToken,
                stateBackupNeighbors);
    }*/

    /**
     * Finds k neighbor flakes in counter clockwise direction.
     * @param childData data received from ZK cache.
     * @return  the map from token to fid of the current neighbors to subscribe
     *            for.
     */
    private SortedMap<Integer, String> extractNeighboursToSubscribeForMessages(
            final List<ChildData> childData) {

        SortedMap<Integer, String> result = new TreeMap<>();

        SortedMap<Integer, String> allFlakes = new TreeMap<>(
                Collections.reverseOrder()); //again reverse because we
                // subscribe instead of them sending. And to make it
                // compatible with message hashing.

        Map<String, FlakeToken> allFlakesConnectData = new HashMap<>();

        for (ChildData child: childData) {
            String path = child.getPath();
            //Integer data = (Integer) Utils.deserialize(child.getData());
            FlakeToken token = (FlakeToken) Utils.deserialize(child.getData());
            String nfid = parseFlakeId(path);
            allFlakes.put(token.getToken(), nfid);
            allFlakesConnectData.put(nfid, token);
            LOGGER.info("CHILDREN: {} , TOKEN: {}", path, token.getToken());
        }

        SortedMap<Integer, String> tail = allFlakes.tailMap(myToken);
        Iterator<Integer> iterator = tail.keySet().iterator();
        iterator.next(); //ignore the self's token.

        int i = 0;
        for (; i < toleranceLevel && iterator.hasNext(); i++) {
            Integer neighborToken = iterator.next();
            String nfid = allFlakes.get(neighborToken);
            result.put(neighborToken, nfid);
            flakeToDataPortMap.put(nfid, allFlakesConnectData.get(nfid));
        }

        Iterator<Integer> headIterator = allFlakes.keySet().iterator();
        for (; i < toleranceLevel && headIterator.hasNext(); i++) {
            Integer neighborToken = headIterator.next();
            String nfid = allFlakes.get(neighborToken);
            result.put(neighborToken, nfid);
            flakeToDataPortMap.put(nfid, allFlakesConnectData.get(nfid));
        }

        LOGGER.info("ME:{}, I WILL BACKUP MSGS FOR: {}", myToken,
                result);
        return result;
    }

    /**
     * Parses the flake id from the full path.
     * @param path path to the flake id which stores the token.
     * @return returns the flake id.
     */
    private String parseFlakeId(final String path) {
        return ZKPaths.getNodeFromPath(path);
    }

    /**
     * @return the list of neighbors to backup msgs for.
     */
    public final List<String> getNeighborsToBackupMsgsFor() {
        return new ArrayList<>(neighborsToBackupMsgsFor.values());
    }

    /**
     * Triggered when initial list of children is cached.
     * This is retrieved synchronously.
     *
     * @param initialChildren initial list of children.
     */
    @Override
    public final void childrenListInitialized(final Collection<ChildData>
                                                     initialChildren) {

    }

    /**
     * Triggered when a new child is added.
     * Note: this is not recursive.
     *
     * @param addedChild newly added child's data.
     */
    @Override
    public final void childAdded(final ChildData addedChild) {
        LOGGER.info("FLAKE ADDED REMOVED");
        ZMQ.Socket controlSoc = getContext().socket(ZMQ.PUSH);
        controlSoc.connect(tokenListenerSockPrefix + getFid());
        byte[] dummy = new byte[]{1};
        controlSoc.send(dummy, 0);
        controlSoc.close();
    }

    /**
     * Triggered when an existing child is removed.
     * Note: this is not recursive.
     *
     * @param removedChild removed child's data.
     */
    @Override
    public final void childRemoved(final ChildData removedChild) {
        LOGGER.info("FLAKE CHILD REMOVED");
        ZMQ.Socket controlSoc = getContext().socket(ZMQ.PUSH);
        controlSoc.connect(tokenListenerSockPrefix + getFid());
        byte[] dummy = new byte[]{1};
        controlSoc.send(dummy, 0);
        controlSoc.close();
    }

    /**
     * Triggered when a child is updated.
     * Note: This is called only when Children data is also cached in
     * addition to stat information.
     *
     * @param updatedChild update child's data.
     */
    @Override
    public final void childUpdated(final ChildData updatedChild) {
        LOGGER.info("FLAKE UPDATED REMOVED");
        ZMQ.Socket controlSoc = getContext().socket(ZMQ.PUSH);
        controlSoc.connect(tokenListenerSockPrefix + getFid());
        byte[] dummy = new byte[]{1};
        controlSoc.send(dummy, 0);
        controlSoc.close();
    }

    /**
     * @return the list of neighbors to be used for state backup.
     *
    public final List<String> getStateBackupNeighbors() {
        return new ArrayList<>(stateBackupNeighbors.values());
    }*/

    /**
     * Recovery (take over) handler class. One instance of this class is
     * responsible for take over of one neighbor flake.
     */
    class RecoveryHandler extends Thread {

        /**
         * Neighbor's flake id that is to be scaled down.
         */
        private final String neighborFid;

        /**
         * Constructor.
         * @param nfid the neighbor's flake id which is to be scaled down.
         */
        public RecoveryHandler(final String nfid) {
            this.neighborFid = nfid;
        }

        /**
         * Thread's run method which is actually responsible for performing
         * all the recover actions.
         */
        @Override
        public void run() {
            //If this is not an immediate neighbor then skip recovery. Give
            // chance to the immediate neighbor first.
            if (!isImmediateNextNeighbor(neighborFid)) {
                return;
            }

            //Step 1. Recover state. I.e. copy the backup state to the state
            // manager corresponding to a given pelletid and given key.

            //Question: Should we start including this in the checkpoint
            // already?

            //Key to pellet state delta mapping.
            Map<String, PelletStateDelta> flakeState
                    = stateManager.getBackupState(neighborFid);

            for (Map.Entry<String, PelletStateDelta> stateEntry
                                        : flakeState.entrySet()) {
                String key = stateEntry.getKey();
                PelletStateDelta receivedDeltaState = stateEntry.getValue();

                //HOW TO GET THE MAPPING FROM KEY TO PELLET INSTANCE ID?
                //EASY. since we removed the requirement for peId.
                PelletState state = stateManager.getState(null, key);

                //state.setLatestTimeStampAtomic(
                //        receivedDeltaState.getTimestamp()); //hmm.. alright i
                        // guess.

                for (Map.Entry<String, Object> delState
                        : receivedDeltaState.getDeltaState().entrySet()) {

                    String statekey = delState.getKey();
                    Object stateVal = delState.getValue();
                    state.setValue(statekey, stateVal);
                }
            }

            //STEP 1 finished. The state is now restored to the pellet state.


            //STEP 2: Send the messages in the queue to the pellet.
            stateManager.startMsgRecovery(neighborFid);

            //STEP 3: Remove the neighbor fid from the ring.
            //3a. remove from ZK.
            //3b. UPDATE THE FLAKE LOCAL STRATEGY.. to unsubscribe/subscribe
            // for new neighbors. BUT HOW?
            //3c. UPDATE THE DISPERSION STRATEGY FOR PREDECESSOR to use the
            // new .. BUT HOW?
            ZKUtils.removeToken(getAppName(), getPelletName(), neighborFid);
        }

        /**
         * @param nfid neighbor flake id
         * @return returns true if the given fid is the immediate neighbor,
         * false otherwise.
         */
        private boolean isImmediateNextNeighbor(final String nfid) {
            SortedMap<Integer, String> tailMap
                    = neighborsToBackupMsgsFor.tailMap(myToken);


            LOGGER.info("My token:{}. Allneighbors:{}, TailMap: {}",
                    myToken, neighborsToBackupMsgsFor, tailMap);

            Iterator<Integer> iterator = tailMap.keySet().iterator();
            //iterator.next();

            Integer firstneighborKey;
            String firstNeighbor;
            if (iterator.hasNext()) {
                firstneighborKey = iterator.next();
                firstNeighbor = tailMap.get(firstneighborKey);
            } else {
                firstneighborKey = neighborsToBackupMsgsFor.firstKey();
                firstNeighbor = neighborsToBackupMsgsFor.get(firstneighborKey);
            }

            if (firstNeighbor.equalsIgnoreCase(nfid)) {
                return true;
            }
            return false;
        }
    }
}
