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

package edu.usc.pgroup.floe.app.pellets;

import java.io.Serializable;

/**
 * @author kumbhare
 */
public class PelletConfiguration implements Serializable {
    /**
     * the state type associated with the pellet.
     */
    private StateType stateType;

    public PelletConfiguration() {
        this.stateType = StateType.LocalOnly;
    }

    /**
     * @return State type configured for the pellet
     */
    public StateType getStateType() {
        return stateType;
    }

    /**
     * Sets the state type for the pellet.
     * @param type state type enum
     */
    public void setStateType(StateType type) {
        this.stateType = type;
    }
}
