package edu.usc.pgroup.floe.examples.pellets;

import edu.usc.pgroup.floe.app.AppContext;
import edu.usc.pgroup.floe.app.Emitter;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.app.pellets.IteratorPellet;
import edu.usc.pgroup.floe.app.pellets.PelletConfiguration;
import edu.usc.pgroup.floe.app.pellets.PelletContext;
import edu.usc.pgroup.floe.app.pellets.StateType;
import edu.usc.pgroup.floe.app.pellets.TupleItertaor;
import edu.usc.pgroup.floe.flake.statemanager.PelletState;
import edu.usc.pgroup.floe.flake.statemanager.StateManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author kumbhare
 */
public class PrintIteratorPellet extends IteratorPellet {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PrintIteratorPellet.class);

    /**
     * Pellet instance id.
     */
    private String peid;

    /**
     * Use to configure different aspects of the pellet,such as state type etc.
     *
     * @param pconf pellet configurer
     */
    @Override
    public final void configure(final PelletConfiguration pconf) {
        pconf.setStateType(StateType.None, null);
    }

    /**
     * The setup function is called once to let the pellet initialize.
     *
     * @param appContext    Application's context. Some data related to
     *                      application's deployment.
     * @param pelletContext Pellet instance context. Related to this
     */
    @Override
    public final void onStart(final AppContext appContext,
                              final PelletContext pelletContext) {
        peid = pelletContext.getPelletInstanceId();
    }

    /**
     * The teardown function, called when the topology is killed.
     * Or when the Pellet instance is scaled down.
     */
    @Override
    public final void teardown() {

    }

    /**
     * @return The names of the streams to be used later during emitting
     * messages.
     */
    @Override
    public final List<String> getOutputStreamNames() {
        return null;
    }

    /**
     * The execute method which is called for each tuple.
     *
     * @param tupleItertaor input tuple received from the preceding pellet.
     * @param emitter       An output emitter which may be used by the user to emmit
     *                      results.
     * @param stateManager  state associated manager associated with the pellet.
     *                      It is the executor's responsiblity to get the state
     */
    @Override
    public final void execute(final TupleItertaor tupleItertaor,
                        final Emitter emitter,
                        final StateManager stateManager) {
        while(true) {
            Tuple tuple = tupleItertaor.next();
            //PelletState state = stateManager.getState(peid, tuple);
            //use state as required.
            if (tuple != null) {
                LOGGER.info("{} : Received: {}",
                        peid,
                        tuple.get("word"));
                emitter.emit(tuple);
            }

        }
    }
}
