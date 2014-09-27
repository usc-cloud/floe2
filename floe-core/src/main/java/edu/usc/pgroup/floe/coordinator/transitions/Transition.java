package edu.usc.pgroup.floe.coordinator.transitions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author kumbhare
 */
public abstract class Transition {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(Transition.class);

    /**
     * List of transition listeners.
     */
    private List<TransitionListener> transitionListenerList;


    /**
     * Start the execution for the given transition. Returns immediately
     * after starting?
     * @param args transaction specific arguments
     */
    public final void executeAsync(final Map<String, Object> args) {
        Thread t = new Thread(
                new Runnable() {
                    /**
                     * This executes in a new thread, which in turn calls the
                     * execute function.
                     */
                    @Override
                    public void run() {
                        try {
                            execute(args);
                        } catch (Exception e) {
                            LOGGER.error("Error occurred while executing "
                                    + "transition: {}. Abandoning transition."
                                    + " Exception: ",
                                    getName(), e);
                        } finally {
                            notifyCompleted();
                        }
                    }
                }
        );

        t.start();
    }

    /**
     * Start the execution for the given transition. Returns immediately
     * after starting?
     * @param args transaction specific arguments
     * @throws Exception if there is an unrecoverable error while
     * processing the transition.
     */
    protected abstract void execute(Map<String, Object> args) throws Exception;

    /**
     * @return gets the name of the transaction.
     */
    public abstract String getName();


    /**
     * Default constructor.
     */
    public Transition() {
        transitionListenerList = new ArrayList<>();
    }

    /**
     * Notify all the listeners that the transition has completed.
     */
    private synchronized void notifyCompleted() {
        for (TransitionListener listener: transitionListenerList) {
            listener.transitionCompleted(this);
        }
    }

    /**
     * Adds a transaction Listener.
     * @param listener a TransitionListener implementation.
     */
    public final synchronized void addTransitionListener(
            final TransitionListener listener) {
        transitionListenerList.add(listener);
    }
}
