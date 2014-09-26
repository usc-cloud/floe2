package edu.usc.pgroup.floe.coordinator.transitions;

import edu.usc.pgroup.floe.coordinator.Coordinator;

/**
 * @author kumbhare
 */
public class ClusterBusyException extends Exception {
    /**
     * Constructor.
     * @param currentStatus Current cluster status (other than "Available")
     */
    public ClusterBusyException(final Coordinator.ClusterStatus currentStatus) {
        super("Cluster is currently busy. Please try again later. "
                + "Current status: " + currentStatus);
    }
}
