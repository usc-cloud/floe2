package edu.usc.pgroup.floe.flake.messaging.dispersion;

import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.flake.FlakeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * @author kumbhare
 */
public class BroadCastDispersion extends MessageDispersionStrategy {


    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(BroadCastDispersion.class);

    /**
     * List of target pellet instances.
     */
    private List<String> allTargetFlakes;

    /**
     * @param appName    Application name.
     * @param pelletName dest pellet name to be used to get data from ZK.
     * @param myFlakeId  Current flake's id.
     */
    public BroadCastDispersion(final String appName,
                               final String pelletName,
                               final String myFlakeId) {
        super(appName, pelletName, myFlakeId);
        allTargetFlakes = new ArrayList<>();
    }

    /**
     * Initializes the strategy.
     *
     * @param args the arguments sent by the user. Fix Me: make this a better
     *             interface.
     */
    @Override
    protected void initialize(final String args) {

    }

    /**
     * Returns the list of target instances to send the given tuple using the
     * defined strategy.
     *
     * @param tuple tuple object.
     * @return the list of target instances to send the given tuple.
     */
    @Override
    public final List<String> getTargetFlakeIds(final Tuple tuple) {
        LOGGER.error("Broadcasting to:{}", allTargetFlakes);
        return allTargetFlakes;
    }

    /**
     * Should return a list of arguments/"envelopes" to be sent along with
     * the message for the given target flake.
     *
     * @param fId one of the flake ids returned by getTargetFlakeIds
     * @return list of arguments to be sent.
     */
    @Override
    public final List<String> getCustomArguments(final String fId) {
        return null;
    }

    /**
     * This function is called exactly once when the initial flake list is
     * fetched.
     *
     * @param flakes list of currently initialized flakes.
     */
    @Override
    public final void initialFlakeList(final List<FlakeToken> flakes) {
        allTargetFlakes.clear();
        List<FlakeToken> cflakes = getCurrentFlakes();
        for (FlakeToken flake: cflakes) {
            allTargetFlakes.add(flake.getFlakeID());
        }
    }

    /**
     * This function is called whenever a new flake is created for the
     * correspondong pellet.
     *
     * @param token flake token corresponding to the added flake.
     */
    @Override
    public final void flakeAdded(final FlakeToken token) {
        allTargetFlakes.clear();
        List<FlakeToken> cflakes = getCurrentFlakes();
        for (FlakeToken flake: cflakes) {
            allTargetFlakes.add(flake.getFlakeID());
        }
    }

    /**
     * This function is called whenever a flake is removed for the
     * correspondong pellet.
     *
     * @param token flake token corresponding to the added flake.
     */
    @Override
    public final void flakeRemoved(final FlakeToken token) {
        allTargetFlakes.clear();
        List<FlakeToken> cflakes = getCurrentFlakes();
        for (FlakeToken flake: cflakes) {
            allTargetFlakes.add(flake.getFlakeID());
        }
    }

    /**
     * This function is called whenever a data associated with a flake
     * corresponding to the given pellet is updated.
     *
     * @param token updated flake token.
     */
    @Override
    public void flakeDataUpdated(final FlakeToken token) {

    }
}
