package edu.usc.pgroup.floe.flake.coordination;

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.flake.FlakeToken;
import edu.usc.pgroup.floe.flake.FlakesTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public class PeerMonitor extends FlakesTracker {

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
     * fid to token map.
     */
    private Map<String, Integer> fidToTokenMap;

    /**
     * List of neighbors for which this peer will backup state and messages.
     */
    private SortedMap<Integer, FlakeToken> allFlakes;


    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ReducerPeerCoordinationComponent.class);

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
        replicationLevel = FloeConfig.getConfig().getInt(
                ConfigProperties.FLAKE_TOLERANCE_LEVEL);
        neighborsToBackupFor = new TreeMap<>(Collections.reverseOrder());
        allFlakes = new TreeMap<>(Collections.reverseOrder());
        fidToTokenMap = new HashMap<>();
        this.flakeId = myFlakeId;
        this.myToken = null;
        start();
    }

    /**
     * This function is called exactly once when the initial flake list is
     * fetched.
     *
     * @param flakes list of currently initialized flakes.
     */
    @Override
    protected final void initialFlakeList(final List<FlakeToken> flakes) {
        for (FlakeToken flake: flakes) {
            if (!allFlakes.containsKey(flake.getToken())) {
                allFlakes.put(flake.getToken(), flake);
                fidToTokenMap.put(flake.getFlakeID(), flake.getToken());
                if (flakeId.equalsIgnoreCase(flake.getFlakeID())) {
                    this.myToken = flake.getToken();
                }
            }
        }
        updateNeighboursToSubscribeForMessages();
    }

    /**
     * This function is called whenever a new flake is created for the
     * correspondong pellet.
     *
     * @param flake flake token corresponding to the added flake.
     */
    @Override
    protected final void flakeAdded(final FlakeToken flake) {
        allFlakes.put(flake.getToken(), flake);
        LOGGER.error(flake.getFlakeID());
        LOGGER.error("{}", flake.getToken());
        LOGGER.error("{}", fidToTokenMap.size());
        fidToTokenMap.put(flake.getFlakeID(), flake.getToken());
        if (flakeId.equalsIgnoreCase(flake.getFlakeID())) {
            this.myToken = flake.getToken();
        }
        updateNeighboursToSubscribeForMessages();
    }

    /**
     * This function is called whenever a flake is removed for the
     * correspondong pellet.
     *
     * @param flake flake token corresponding to the added flake.
     */
    @Override
    protected final void flakeRemoved(final FlakeToken flake) {
        if (allFlakes.containsKey(flake.getToken())) {
            allFlakes.remove(flake.getToken());
        }
        if (fidToTokenMap.containsKey(flake.getFlakeID())) {
            fidToTokenMap.remove(flake.getFlakeID());
        }
        updateNeighboursToSubscribeForMessages();
    }

    /**
     * This function is called whenever a data associated with a flake
     * corresponding to the given pellet is updated.
     *
     * @param flake updated flake token.
     */
    @Override
    protected final void flakeDataUpdated(final FlakeToken flake) {

        Integer prevToken = null;
        if (fidToTokenMap.containsKey(flake.getFlakeID())) {
            prevToken = fidToTokenMap.get(flake.getFlakeID());
            fidToTokenMap.remove(flake.getFlakeID());
        }

        if (prevToken != null
                && prevToken != flake.getToken()
                && allFlakes.containsKey(prevToken)) {
            allFlakes.remove(prevToken);
        }

        allFlakes.put(flake.getToken(), flake);
        fidToTokenMap.put(flake.getFlakeID(), flake.getToken());

        if (flakeId.equalsIgnoreCase(flake.getFlakeID())) {
            this.myToken = flake.getToken();
        }

        updateNeighboursToSubscribeForMessages();
    }


    /**
     * Finds k neighbor flakes in counter clockwise direction.
     * the map from token to fid of the current neighbors to subscribe for
     * TODOX: THIS HAS PERF. ISSUES CAN BE IMPROVED.
     */
    private void
            updateNeighboursToSubscribeForMessages() {

        if (myToken == null) { return; } //not all info. is available yet.

        SortedMap<Integer, FlakeToken> tail = allFlakes.tailMap(myToken);
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
            FlakeToken nfk = allFlakes.get(neighborToken);
            if (!nfk.getFlakeID().equalsIgnoreCase(flakeId)) {
                result.put(neighborToken, nfk);
            }
        }

        Iterator<Integer> headIterator = allFlakes.keySet().iterator();
        for (; i < replicationLevel && headIterator.hasNext(); i++) {
            Integer neighborToken = headIterator.next();
            FlakeToken nfk = allFlakes.get(neighborToken);
            if (!nfk.getFlakeID().equalsIgnoreCase(flakeId)) {
                result.put(neighborToken, nfk);
            }
        }

        LOGGER.info("ME:{}, I WILL BACKUP MSGS FOR: {}", myToken,
                result);
        synchronized (neighborsToBackupFor) {
            neighborsToBackupFor.clear();
            neighborsToBackupFor.putAll(result);
        }
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
}
