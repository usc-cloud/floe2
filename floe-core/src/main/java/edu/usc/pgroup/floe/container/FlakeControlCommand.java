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

package edu.usc.pgroup.floe.container;

import java.io.Serializable;

/**
 * The request commands to be sent to Flake instances.
 * @author kumbhare
 */
public class FlakeControlCommand implements Serializable {
    /**
     * List of commands the container can send to the Flake.
     */
    public enum Command {
        /**
         * Command to increment the number of pellet instances.
         */
        INCREMENT_PELLET,
        /**
         * If the predecessor scales out, this PE should connect to the newly
         * created Flake.
         */
        CONNECT_PRED,
        /**
         * If the predecessors scales in (i.e. a Flake is destroyed),
         * this PE should disconnect gracefully.
         */
        DISCONNECT_PRED,
        /**
         * Command to decrement the number of pellet instances.
         */
        DECREMENT_PELLET,
        /**
         * Command to start the 'ready' but not 'running' pellets.
         */
        START_PELLETS,
        /**
         * Command to switch the pellet's active alternate.
         */
        SWITCH_ALTERNATE,
        /**
         * Pellet signal command.
         */
        PELLET_SIGNAL,
        /**
         * FLAKE command to decrement all pellets.
         */
        DECREMENT_ALL_PELLETS,
        /**
         * Flake command to update subscription for message backup.
         */
        UPDATE_SUBSCRIPTION,
        /**
         * Command to terminate self.
         */
        TERMINATE
    }

    /**
     * The command to send to the Flake.
     */
    private Command command;


    /**
     * The custom data to send to the Flake.
     */
    private Object data;

    /**
     * Constructor.
     * @param flakeCommand command to send to the Flake.
     * @param commandData custom command data. (FIXME: No check is done here.)
     */
    public FlakeControlCommand(final Command flakeCommand,
                               final Object commandData) {
        this.command = flakeCommand;
        this.data = commandData;
    }

    /**
     * @return the command to execute.
     */
    public final Command getCommand() {
        return command;
    }

    /**
     * @return the custom data.
     */
    public final Object getData() {
        return data;
    }

    /**
     * @return the string representation of the command;
     */
    @Override
    public final String toString() {
        return command.toString();
    }
}
