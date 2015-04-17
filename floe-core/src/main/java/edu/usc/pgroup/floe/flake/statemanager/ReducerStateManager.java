package edu.usc.pgroup.floe.flake.statemanager;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.flake.FlakeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author kumbhare
 */
public class ReducerStateManager implements StateManager {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ReducerStateManager.class);

    /**
     * Key field name used by the reducer for grouping.
     */
    private String keyFieldName;

    /**
     * The pellet instance id to pellet state map. PelletState map is a map
     * from the custom key identifier to the pellet state.
     *
     private ConcurrentHashMap<String,
     HashMap<Object, PelletState>> pelletStateMap;*/

    //To Fix #39. Since we do not know the pellet instance id during
    // recovery, we cannot associate state with a given pellet instance.
    // Further, we dont really use pe instance id since a state object is
    // associated with each key anyways. That was used only for perf. i guess.

    /**
     * PelletState map is a map from the reducer key identifier to the pellet
     * state.
     */
    private ConcurrentHashMap<Object, PelletState> pelletStateMap;


    /**
     * Initialize the state manager.
     *
     * @param appName flake's id.
     * @param pelletName flake's id.
     * @param flakeId flake's id.
     * @param args string encoded list of arguments.
     */
    @Override
    public final void init(final String appName,
                           final String pelletName,
                           final String flakeId, final String args) {
        keyFieldName = args;
        pelletStateMap = new ConcurrentHashMap<>();
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

        String value = (String) tuple.get(keyFieldName);

        return getState(peId, value);
    }

    /**
     * Returns the object (state) associated with the given local pe instance.
     * The tuple may be used to further divide the state (e.g. in case of
     * reducer pellet, the State's key will be used to divide the state).
     * NOTE: This is a state specific key, AND NOT NECESSARILY SAME AS THE
     * MESSAGE KEY.
     *
     * @param peId Pellet's instance id.
     * @param keyValue  The value associated with the corresponding field name
     *                  (this is used during recovery since we do not have
     *                  access to the entire tuple, but just the key).
     * @return pellet state corresponding to the given peId and key value
     * combination.
     */
    @Override
    public final PelletState getState(final String peId,
                                final String keyValue) {
        synchronized (pelletStateMap) {
            if (!pelletStateMap.containsKey(keyValue)) {
                LOGGER.error("Creating new state for key: {}", keyValue);
                pelletStateMap.put(keyValue, new PelletState(keyValue));
            }
        }

        return pelletStateMap.get(keyValue);
    }

    /**
     * Checkpoint state and return the serialized delta to send to the backup
     * nodes.
     *
     * @param neighborFlakeId the flake id to which the checkpoint is to be
     *                        backed up.
     * @return serialized delta to send to the backup nodes.
     */
    @Override
    public final byte[] getIncrementalStateCheckpoint(
            final String neighborFlakeId) {
        Kryo kryo = new Kryo();
        ByteArrayOutputStream outStream = new ByteArrayOutputStream();
        Output kryoOut = new Output(outStream);

        synchronized (pelletStateMap) {
            for (PelletState pState: pelletStateMap.values()) {
                LOGGER.debug("starting checkpointing:{}",
                        pState.getCustomId());

                if (pState.getLatestTimeStampAtomic() == -1) {
                    continue; //do not checkpoint this state yet.
                }

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

        kryoOut.flush();

        byte[] chkpt = outStream.toByteArray();
        //checkptSizemeter.mark(chkpt.length);

        kryoOut.close();

        return chkpt;
    }

    /**
     * Used to backup the states received from the neighbor flakes.
     *
     * @param nfid           flake id of the neighbor from which the state
     *                       update is received.
     * @param checkpointdata the checkpoint data received from the
     */
    @Override
    public final void storeNeighborCheckpointToBackup(final String nfid,
                                                final byte[] checkpointdata) {
        //TODO
    }

    /**
     * Retrieve the state backed up for the given neighbor flake id.
     *
     * @param neighborFid neighbor's flake id.
     * @param keys        List of keys to be moved from the backup to the
     *                    primary.
     * @return the backedup state assocuated with the given fid
     */
    @Override
    public final Map<String, PelletStateDelta> copyBackupToPrimary(
            final String neighborFid, final List<String> keys) {
        return null;
    }

    /**
     * Repartitions the state. Is used during state migrations for
     * loadbalance, scale in/out etc.
     *
     * @param selfFid      Flake id for the current flake.
     * @param neighborTokens list of neighbor flake ids that hold the backup
     *                     for the current flake.
     * @return a map for neighbor/self fids to the list of state keys to be
     * transferred to that neighbor.
     * NOTE: THESE NEIGHBORS ARE ONLY THOSE WHO ALREADY HOLD THE "BACKUP",
     * the number of such neighbors depend on the "replication" factor.
     * Typically 1.
     */
    @Override
    public final Map<String, List<String>> repartitionState(
            final String selfFid, final List<FlakeToken> neighborTokens) {
        return null;
    }

    /**
     * Return the position (token number) between -INT_MAX to INT_MAX for the
     * new flake while scaling out.
     *
     * @param myToken             own flake's token.
     * @param neighborsToBackupOn list of neighbors that this flake uses to
     *                            backup it's data.
     * @return return the new token value.
     */
    @Override
    public final Integer getTokenForNewFlake(
            final FlakeToken myToken,
            final SortedMap<Integer, FlakeToken> neighborsToBackupOn) {
        return null;
    }
}
