package edu.usc.pgroup.floe.coordinator.transitions;

import edu.usc.pgroup.floe.coordinator.Coordinator;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import edu.usc.pgroup.floe.zookeeper.ZKUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author kumbhare
 * TODO: Transition and execute may be a misnomer. Change this later.
 */
public final class Transitions {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(Transitions.class);

    /**
     * hiding the default constructor.
     */
    private Transitions() {

    }

    /**
     * Transition to be executed at the cluster level. e.g. start/stop a new
     * application.
     * Start the execution in a new thread.
     * Uses ZK for coordination.
     * Returns immediately.
     * @param transition cluster transition to be executed.
     * @param args arguments
     * @throws Exception Exception wrapper for any exceptions that occur
     * during the transition.
     */
    public static synchronized void execute(final ClusterTransition transition,
                                            final Map<String, Object> args)
    throws Exception {

        //Get the cluster status. If it is not AVAILABLE. throw an exception.
        // Else. Lock the ZK field, set the status to Busy and start the
        // execution of the transaction.

        //also set the transaction completed listener.
        byte[] bstatus = ZKClient.getInstance().getCuratorClient().getData()
                .forPath(ZKUtils.getClusterStatusPath());
        Coordinator.ClusterStatus clusterStatus
                = (Coordinator.ClusterStatus) Utils.deserialize(bstatus);

        if (clusterStatus != Coordinator.ClusterStatus.AVAILABLE) {
            throw new ClusterBusyException(clusterStatus);
        }

        ZKUtils.setClusterStatus(Coordinator.ClusterStatus.BUSY);

        transition.addTransitionListener(new TransitionListener() {
            @Override
            public void transitionCompleted(final Transition transition) {
                try {
                    LOGGER.info("Transition Completed.");
                    ZKUtils.setClusterStatus(
                            Coordinator.ClusterStatus.AVAILABLE);
                } catch (Exception e) {
                    LOGGER.error("Error occurred while updating the cluster "
                            + "status. Any updates from now on might be "
                            + "invalid. Restart the Coordinator to resume "
                            + "operations. Exception: {}", e);
                }
            }
        });

        try {
            transition.executeAsync(args);
        } catch (Exception e) {
            LOGGER.warn("Error occurred while executing transaction: "
                    + transition.getName());
        }
    }

    /**
     * Transition to be executed at the cluster level. e.g. start/stop a new
     * application.
     * Start the execution in a new thread.
     * Uses ZK for coordination.
     * Returns immediately.
     * @param transition cluster transition to be executed.
     * @param appName Application name for which the given transition is to
     *                be executed.
     * @param args arguments
     * @throws Exception Exception wrapper for any exceptions that occur
     * during the transition.
     */
    public static synchronized void execute(final AppTransition transition,
                                            final String appName,
                                            final Map<String, Object> args)
            throws Exception {
        transition.execute(args);
    }
}
