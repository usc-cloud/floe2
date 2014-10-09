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
import edu.usc.pgroup.floe.flake.FlakeToken;
import edu.usc.pgroup.floe.flake.statemanager.PelletStateDelta;
import edu.usc.pgroup.floe.flake.statemanager.StateManagerComponent;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.utils.ZKPaths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
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
public class ReducerCoordinationComponent extends CoordinationComponent {

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
     */
    private final SortedMap<Integer, String> stateBackupNeighbors;


    /**
     * Fixme. Should not be string.
     * K = toleranceLevel Neighbor flakes in counter clockwise direction.
     */
    private final SortedMap<Integer, String> neighborsToBackupMsgsFor;

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
    private PathChildrenCache flakeCache;

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
        this.stateBackupNeighbors = new TreeMap<>();
        this.neighborsToBackupMsgsFor = new TreeMap<>();
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

        flakeCache = new PathChildrenCache(ZKClient.getInstance()
                .getCuratorClient(), pelletTokenPath, true);

        boolean success = true;

        ZMQ.Socket stateSoc = getContext().socket(ZMQ.SUB);

        try {
            flakeCache.start();
            flakeCache.rebuild();


            List<ChildData> childData = flakeCache.getCurrentData();

            //extractNeighboursToBackupState(childData);
            extractNeighboursToSubscribeForMessages(childData);


            /**
             * ZMQ socket connection publish the state to the backups.
             */
            if (flakeToDataPortMap != null) {
                for (Map.Entry<String, FlakeToken> connctInfo
                        : flakeToDataPortMap.entrySet()) {

                    String ssConnetStr
                            = Utils.Constants.FLAKE_STATE_SUB_SOCK_PREFIX
                            + connctInfo.getValue().getIpOrHost() + ":"
                            + connctInfo.getValue().getStateCheckptPort();

                    stateSoc.subscribe(connctInfo.getKey().getBytes());

                    LOGGER.info("connecting STATE CHECKPOINTER "
                             + "to listen for state updates: {}", ssConnetStr);

                    stateSoc.connect(ssConnetStr);
                }
            }

        } catch (Exception e) {
            LOGGER.error("Could not start token monitor.{}", e);
            success = false;
        }

        success = true;

        notifyStarted(success);

        ZMQ.Poller pollerItems = new ZMQ.Poller(2);
        pollerItems.register(terminateSignalReceiver, ZMQ.Poller.POLLIN);
        pollerItems.register(stateSoc, ZMQ.Poller.POLLIN);

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
                LOGGER.info("State delta received from:{}", nfid);
                byte[] serializedState = stateSoc.recv();

                List<PelletStateDelta> deltas
                        = extractPelletStateDeltas(serializedState);

                stateManager.backupState(nfid, deltas);
                //stateManager.backupState(nfid, deltas);
            }

            //Check for termination here..

            //byte[] checkpointdata = stateManager.checkpointState();
        }

        try {
            flakeCache.close();
        } catch (IOException e) {
            e.printStackTrace();
            success = false;
        }
        notifyStopped(success);
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
                    LOGGER.info("received:{}",
                            pes.getDeltaState());
                    deltas.add(pes);
                }
                LOGGER.info("Finished deserialization.");
                kryoIn.close();
            } catch (Exception e) {
                LOGGER.warn("Exception: {}", e);
            }
        } else {
            LOGGER.info("No new updates to checkpoint.");
        }

        return deltas;
    }

    /**
     * Finds k neighbor flakes in counter clockwise direction.
     * @param childData data received from ZK cache.
     */
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
    }

    /**
     * Waits for all upstream neighbors to ping. i.e. those neighbors whose
     * data and messages will be backed up on this flake.
     */
    private void waitForAllNeighborsPing() {
    }

    /**
     * Finds k neighbor flakes in counter clockwise direction.
     * @param childData data received from ZK cache.
     */
    private void extractNeighboursToSubscribeForMessages(
            final List<ChildData> childData) {

        SortedMap<Integer, String> allFlakes = new TreeMap<>();
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
            neighborsToBackupMsgsFor.put(neighborToken, nfid);
            flakeToDataPortMap.put(nfid, allFlakesConnectData.get(nfid));
        }

        Iterator<Integer> headIterator = allFlakes.keySet().iterator();
        for (; i < toleranceLevel && headIterator.hasNext(); i++) {
            Integer neighborToken = headIterator.next();
            neighborsToBackupMsgsFor.put(neighborToken,
                    allFlakes.get(neighborToken));
        }

        LOGGER.info("ME:{}, I WILL BACKUP MSGS FOR: {}", myToken,
                neighborsToBackupMsgsFor);
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
     * @return the list of neighbors to be used for state backup.
     */
    public final List<String> getStateBackupNeighbors() {
        return new ArrayList<>(stateBackupNeighbors.values());
    }
}
