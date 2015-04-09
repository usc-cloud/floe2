package edu.usc.pgroup.floe.flake.coordination;

import edu.usc.pgroup.floe.flake.FlakeToken;

import java.util.SortedMap;

/**
 * @author kumbhare
 */
public interface PeerUpdateListener {
    /**
     * The function is called whenever a peer is added or removed for this
     * flake.
     * @param newPeers list of newly added peers.
     * @param removedPeers list of removed peers.
     */
    void peerListUpdated(final SortedMap<Integer, FlakeToken> newPeers,
                         final SortedMap<Integer, FlakeToken> removedPeers);
}
