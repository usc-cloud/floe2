package edu.usc.pgroup.floe.flake.coordination;

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.flake.FlakeToken;
import edu.usc.pgroup.floe.flake.FlakeUpdateListener;
import edu.usc.pgroup.floe.flake.FlakesTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class PeerMonitor extends FlakesTracker implements FlakeUpdateListener {

    /**
     * This flake's id.
     */
    private final String flakeId;

    /**
     * Tolerance/replication level.
     */
    private final int replicationLevel;

    /**
     * This flake's current token.
     */
    private Integer myToken;

    /**
     * List of neighbors for which this peer will backup state and messages.
     */
    private SortedMap<Integer, FlakeToken> neighborsToBackupFor;

    /**
     * List of neighbors on which this Flake should backup it's data.
     */
    private SortedMap<Integer, FlakeToken> myBackupNeighbors;

    /**
     * fid to token map.
     */
    private Map<String, Integer> fidToTokenMap;

    /**
     * All flakes in reverse order of token.
     */
    private SortedMap<Integer, FlakeToken> allFlakesReverse;

    /**
     * All flakes in forward order of token.
     */
    private SortedMap<Integer, FlakeToken> allFlakesForward;


    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PeerMonitor.class);

    /**
     * List of peer update listeners.
     */
    private List<PeerUpdateListener> peerUpdateListeners;

    /**
     * Constructor.
     *
     * @param appName    name of the application.
     * @param pelletName name of the pellet whose flakes has to be tracked.
     * @param myFlakeId this flake's id.
     */
    public PeerMonitor(final String appName, final String pelletName,
                       final String myFlakeId) {
        super(appName, pelletName);
        addFlakeUpdateListener(this);
        replicationLevel = FloeConfig.getConfig().getInt(
                ConfigProperties.FLAKE_TOLERANCE_LEVEL);
        neighborsToBackupFor = new TreeMap<>(Collections.reverseOrder());
        myBackupNeighbors = new TreeMap<>(Collections.reverseOrder());
        allFlakesReverse = new TreeMap<>(Collections.reverseOrder());
        allFlakesForward = new TreeMap<>();
        fidToTokenMap = new HashMap<>();
        this.flakeId = myFlakeId;
        this.myToken = null;
        peerUpdateListeners = new ArrayList<>();
    }

    /**
     * Adds a peer update listener.
     * @param listener peer update listener instance.
     */
    public final void addPeerUpdateListener(final PeerUpdateListener listener) {
        synchronized (peerUpdateListeners) {
            LOGGER.info("ADDING LISTERNER:{}", listener);
            peerUpdateListeners.add(listener);
        }
    }

    /**
     * This function is called exactly once when the initial flake list is
     * fetched.
     *
     * @param flakes list of currently initialized flakes.
     */
    @Override
    public final void initialFlakeList(final List<FlakeToken> flakes) {
        for (FlakeToken flake: flakes) {
            allFlakesForward.put(flake.getToken(), flake);
            allFlakesReverse.put(flake.getToken(), flake);

            fidToTokenMap.put(flake.getFlakeID(), flake.getToken());
            if (flakeId.equalsIgnoreCase(flake.getFlakeID())) {
                this.myToken = flake.getToken();
            }
        }
        updateNeighbors();
    }

    /**
     * This function is called whenever a new flake is created for the
     * correspondong pellet.
     *
     * @param flake flake token corresponding to the added flake.
     */
    @Override
    public  final void flakeAdded(final FlakeToken flake) {
        allFlakesForward.put(flake.getToken(), flake);
        allFlakesReverse.put(flake.getToken(), flake);

        LOGGER.info(flake.getFlakeID());
        LOGGER.info("{}", flake.getToken());
        LOGGER.info("{}", fidToTokenMap.size());

        fidToTokenMap.put(flake.getFlakeID(), flake.getToken());

        if (flakeId.equalsIgnoreCase(flake.getFlakeID())) {
            this.myToken = flake.getToken();
        }
        updateNeighbors();
    }

    /**
     * This function is called whenever a flake is removed for the
     * correspondong pellet.
     *
     * @param flake flake token corresponding to the added flake.
     */
    @Override
    public  final void flakeRemoved(final FlakeToken flake) {
        if (allFlakesForward.containsKey(flake.getToken())) {
            allFlakesForward.remove(flake.getToken());
        }
        if (allFlakesForward.containsKey(flake.getToken())) {
            allFlakesForward.remove(flake.getToken());
        }
        if (fidToTokenMap.containsKey(flake.getFlakeID())) {
            fidToTokenMap.remove(flake.getFlakeID());
        }
        updateNeighbors();
    }

    /**
     * This function is called whenever a data associated with a flake
     * corresponding to the given pellet is updated.
     *
     * @param flake updated flake token.
     */
    @Override
    public  final void flakeDataUpdated(final FlakeToken flake) {

        Integer prevToken = null;
        if (fidToTokenMap.containsKey(flake.getFlakeID())) {
            prevToken = fidToTokenMap.get(flake.getFlakeID());
            fidToTokenMap.remove(flake.getFlakeID());
        }

        if (prevToken != null
                && prevToken != flake.getToken()
                && allFlakesReverse.containsKey(prevToken)) {
            allFlakesReverse.remove(prevToken);
        }

        if (prevToken != null
                && prevToken != flake.getToken()
                && allFlakesForward.containsKey(prevToken)) {
            allFlakesForward.remove(prevToken);
        }

        allFlakesForward.put(flake.getToken(), flake);
        allFlakesReverse.put(flake.getToken(), flake);

        fidToTokenMap.put(flake.getFlakeID(), flake.getToken());

        if (flakeId.equalsIgnoreCase(flake.getFlakeID())) {
            this.myToken = flake.getToken();
        }

        updateNeighbors();
    }

    /**
     * Update both neighbors to backup for and neighbors to backup on.
     */
    private synchronized void updateNeighbors() {
        updateNeighboursToSubscribeForMessages();
        updateNeighboursToBackupOn();
    }

    /**
     * Finds k neighbor flakes in counter clockwise direction.
     * the map from token to fid of the current neighbors to subscribe for
     * TODOX: THIS HAS PERF. ISSUES CAN BE IMPROVED.
     */
    private synchronized void
            updateNeighboursToSubscribeForMessages() {

        if (myToken == null) { return; } //not all info. is available yet.

        SortedMap<Integer, FlakeToken> tail = allFlakesReverse.tailMap(myToken);
        Iterator<Integer> iterator = tail.keySet().iterator();
        iterator.next(); //ignore the self's token.

        /**
         * List of neighbors for which this peer will backup state and messages.
         */
        SortedMap<Integer, FlakeToken> result = new TreeMap<>(
                                                    Collections.reverseOrder());

        int i = 0;
        for (; i < replicationLevel && iterator.hasNext(); i++) {
            Integer neighborToken = iterator.next();
            FlakeToken nfk = allFlakesReverse.get(neighborToken);
            if (!nfk.getFlakeID().equalsIgnoreCase(flakeId)) {
                result.put(neighborToken, nfk);
            }
        }

        Iterator<Integer> headIterator = allFlakesReverse.keySet().iterator();
        for (; i < replicationLevel && headIterator.hasNext(); i++) {
            Integer neighborToken = headIterator.next();
            FlakeToken nfk = allFlakesReverse.get(neighborToken);
            if (!nfk.getFlakeID().equalsIgnoreCase(flakeId)) {
                result.put(neighborToken, nfk);
            }
        }

        SortedMap<Integer, FlakeToken> added
                = getNewlyAddedFlakes(result, neighborsToBackupFor);
        SortedMap<Integer, FlakeToken> removed
                = getRemovedFlakes(result, neighborsToBackupFor);

        LOGGER.debug("ME:{}, I WILL BACKUP MSGS FOR: A:{} R:{} NEW:{}, old:{},"
                        + "listeners:{}", myToken, added, removed, result,
                                    neighborsToBackupFor, peerUpdateListeners);

        synchronized (neighborsToBackupFor) {
        if (peerUpdateListeners != null) {
            for (PeerUpdateListener listener: peerUpdateListeners) {
                LOGGER.debug("CALLING LISTENERS",
                        myToken, added, removed, result, neighborsToBackupFor);
                listener.peerListUpdated(added, removed);
            }
        }


            neighborsToBackupFor.clear();
            neighborsToBackupFor.putAll(result);
        }
    }

    /**
     * Finds k neighbor flakes in counter clockwise direction.
     * the map from token to fid of the current neighbors to subscribe for
     * TODOX: THIS HAS PERF. ISSUES CAN BE IMPROVED.
     */
    private synchronized void
            updateNeighboursToBackupOn() {

        if (myToken == null) { return; } //not all info. is available yet.

        SortedMap<Integer, FlakeToken> tail = allFlakesForward.tailMap(myToken);
        Iterator<Integer> iterator = tail.keySet().iterator();
        iterator.next(); //ignore the self's token.

        /**
         * List of neighbors for which this peer will backup state and messages.
         */
        SortedMap<Integer, FlakeToken> result = new TreeMap<>();

        int i = 0;
        for (; i < replicationLevel && iterator.hasNext(); i++) {
            Integer neighborToken = iterator.next();
            FlakeToken nfk = allFlakesForward.get(neighborToken);
            if (!nfk.getFlakeID().equalsIgnoreCase(flakeId)) {
                result.put(neighborToken, nfk);
            }
        }

        Iterator<Integer> headIterator = allFlakesForward.keySet().iterator();
        for (; i < replicationLevel && headIterator.hasNext(); i++) {
            Integer neighborToken = headIterator.next();
            FlakeToken nfk = allFlakesForward.get(neighborToken);
            if (!nfk.getFlakeID().equalsIgnoreCase(flakeId)) {
                result.put(neighborToken, nfk);
            }
        }

        LOGGER.debug("ME:{}, I WILL BACKUP MSGS FOR: {}", myToken,
                result);

        synchronized (myBackupNeighbors) {
            myBackupNeighbors.clear();
            myBackupNeighbors.putAll(result);
        }
    }

    /**
     * Gets the newly added neighbors.
     * @param currentNeighbors current neighbors obtained from ZK.
     * @param oldNeighbors old neighbors.
     * @return the newly added neighbors.
     */
    private SortedMap<Integer, FlakeToken> getNewlyAddedFlakes(
            final SortedMap<Integer, FlakeToken> currentNeighbors,
            final SortedMap<Integer, FlakeToken> oldNeighbors) {
        SortedMap<Integer, FlakeToken> added = new TreeMap<>();

        for (Map.Entry<Integer, FlakeToken> current
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
    private SortedMap<Integer, FlakeToken> getRemovedFlakes(
            final SortedMap<Integer, FlakeToken> currentNeighbors,
            final SortedMap<Integer, FlakeToken> oldNeighbors) {
        SortedMap<Integer, FlakeToken> removed = new TreeMap<>();

        for (Map.Entry<Integer, FlakeToken> old
                : oldNeighbors.entrySet()) {
            if (currentNeighbors.containsKey(old.getKey())) {
                continue;
            }
            removed.put(old.getKey(), old.getValue());
        }
        return removed;
    }

    /**
     * @return a list of k neighbors (as a sorted map)
     */
    public final SortedMap<Integer, FlakeToken> getNeighborsToBackupFor() {
        return neighborsToBackupFor;
    }

    /**
     * @return the current fid to token map.
     */
    public final Map<String, Integer> getFidToTokenMap() {
        return fidToTokenMap;
    }

    /**
     * @return the list of neighbors to backup on.
     */
    public final SortedMap<Integer, FlakeToken> getNeighborsToBackupOn() {
        return myBackupNeighbors;
    }
}
