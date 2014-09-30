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

package edu.usc.pgroup.floe.flake.messaging.dispersion;

/**
 * @author kumbhare
 */
public interface BackChannelReceiver {
    /**
     * Call back whenever a message is received from a target pellet instance
     * on the back channel. This can be used by dispersion strategy to choose
     * the target instance to send the message to.
     * @param targetFlakeId pellet instance id from which the
     *                               message is received.
     * @param message message body.
     * @param toContinue true if the flake is sending a regular backchannel
     *                   msg. False if the message is sent on scaling down i
     *                   .e. 'terminate' is called on the target flake.
     */
    void backChannelMessageReceived(String targetFlakeId,
                                    byte[] message, Boolean toContinue);
}
