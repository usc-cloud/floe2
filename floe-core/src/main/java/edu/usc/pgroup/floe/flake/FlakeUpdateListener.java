package edu.usc.pgroup.floe.flake;

import java.util.List;

/**
 * @author kumbhare
 */
public interface FlakeUpdateListener {
    /**
     * This function is called exactly once when the initial flake list is
     * fetched.
     * @param flakes list of currently initialized flakes.
     */
    void initialFlakeList(List<FlakeToken> flakes);

    /**
     * This function is called whenever a new flake is created for the
     * correspondong pellet.
     * @param token flake token corresponding to the added flake.
     */
    void flakeAdded(FlakeToken token);

    /**
     * This function is called whenever a flake is removed for the
     * correspondong pellet.
     * @param token flake token corresponding to the added flake.
     */
    void flakeRemoved(FlakeToken token);

    /**
     * This function is called whenever a data associated with a flake
     * corresponding to the given pellet is updated.
     * @param token updated flake token.
     */
    void flakeDataUpdated(FlakeToken token);
}
