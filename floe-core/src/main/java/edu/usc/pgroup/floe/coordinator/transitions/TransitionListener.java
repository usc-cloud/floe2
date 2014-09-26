package edu.usc.pgroup.floe.coordinator.transitions;

/**
 * @author kumbhare
 */
public interface TransitionListener {
    /**
     * called when the transition is completed.
     * @param transition the transition that was completed.
     */
    void transitionCompleted(Transition transition);
}
