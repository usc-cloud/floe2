/*
 * Copyright 2014 University of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package edu.usc.pgroup.floe.flake.statemanager;

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.flake.FlakeComponent;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * @author kumbhare
 */
public class StateCheckpointComponent extends FlakeComponent {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(StateCheckpointComponent.class);

    /**
     * CHeckpointing period.
     */
    private final int checkpointPeriod;

    /**
     * State manager instance.
     */
    private final StateManagerComponent stateManager;

    /**
     * Port to bind the socket to send periodic state checkpoints.
     */
    private int port;

    /**
     * Constructor.
     *
     * @param flakeId       Flake's id to which this component belongs.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     * @param stateMgr State manager component.
     * @param stateChkptPort port to use for connections to checkpoint state.
     */
    public StateCheckpointComponent(final String flakeId,
                                    final String componentName,
                                    final ZMQ.Context ctx,
                                    final StateManagerComponent stateMgr,
                                    final int stateChkptPort) {
        super(flakeId, componentName, ctx);
        this.stateManager = stateMgr;
        this.port = stateChkptPort;
        this.checkpointPeriod = FloeConfig.getConfig().getInt(ConfigProperties
                .FLAKE_STATE_CHECKPOINT_PERIOD) * Utils.Constants.MILLI;

    }

    /**
     * Starts all the sub parts of the given component and notifies when
     * components starts completely. This will be in a different thread,
     * so no need to worry.. block as much as you want.
     *
     * @param terminateSignalReceiver terminate signal receiver.
     */
    @Override
    protected final void runComponent(
            final ZMQ.Socket terminateSignalReceiver) {
        notifyStarted(true);

        ZMQ.Poller pollerItems = new ZMQ.Poller(1);
        pollerItems.register(terminateSignalReceiver, ZMQ.Poller.POLLIN);

        /**
         * ZMQ socket connection publish the state to the backups.
         */
        ZMQ.Socket stateSoc = getContext().socket(ZMQ.PUB);
        String ssConnetStr = Utils.Constants.FLAKE_STATE_PUB_SOCK + port;
        LOGGER.info("binding STATE CHECKPOINTER to socket at: {}", ssConnetStr);
        stateSoc.bind(ssConnetStr);

        notifyStarted(true);

        while (!Thread.currentThread().isInterrupted()) {

            int polled = pollerItems.poll(checkpointPeriod);
            if (pollerItems.pollin(0)) {
                //terminate.
                LOGGER.warn("Terminating state checkpointing");
                terminateSignalReceiver.recv();
                break;
            }

            LOGGER.debug("Checkpointing State");
            byte[] checkpointdata = stateManager.checkpointState();

            /*if (checkpointdata != null && checkpointdata.length > 0) {
                try {
                    Kryo kryo = new Kryo();
                    Input kryoIn = new Input(checkpointdata);

                    //stateSerializer.setBuffer(checkpointdata);
                    //PelletStateDelta pes = stateSerializer.getNextState();

                    while (!kryoIn.eof()) {
                        PelletStateDelta pes
                                = kryo.readObject(kryoIn,
                                                    PelletStateDelta.class);

                        LOGGER.info("Sample deserialization:{}",
                                pes.getDeltaState());
                    }
                    kryoIn.close();
                } catch (Exception e) {
                    LOGGER.warn("Exception: {}", e);
                }
            }*/
            stateSoc.sendMore(getFid());
            stateSoc.send(checkpointdata, 0);
        }

        notifyStopped(true);
    }
}
