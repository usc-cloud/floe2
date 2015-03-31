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

import edu.usc.pgroup.floe.flake.statemanager.GenericPelletStateManager;
import edu.usc.pgroup.floe.flake.statemanager.ReducerStateManager;

import java.io.Serializable;

/**
 * @author kumbhare
 */
public class PelletConfiguration implements Serializable {
    /**
     * the state type associated with the pellet.
     */
    private StateType stateType;

    /**
     * Additional state parameters that can be set.
     */
    private String stateParams;

    /**
     * State manager Class.
     */
    private String stateManagerClass;

    /**
     * Default constructor. (sets the state type to localonly).
     */
    public PelletConfiguration() {
        this.stateType = StateType.LocalOnly;
    }

    /**
     * @return State type configured for the pellet.
     */
    public final StateType getStateType() {
        return stateType;
    }

    /**
     * Sets the state type for the pellet.
     * @param type state type enum.
     * @param stParams string encoded parameters to be passed to the state
     *                    manager's init function.
     */
    public final void setStateType(final StateType type,
                                   final String stParams) {
        this.stateType = type;
        this.stateParams = stParams;
        switch (stateType) {
            case LocalOnly:
                this.stateManagerClass
                        = GenericPelletStateManager.class.getCanonicalName();
                break;
            case Reduce:
                this.stateManagerClass
                        = ReducerStateManager.class.getCanonicalName();
                break;
            default:
                this.stateManagerClass = null;

        }
    }

    /**
     * Sets the custom state manager for the pellet.
     * @param stManagerClass fully qualified name for the manager class
     * @param stParams string encoded parameters to be passed to the state
     *                    manager's init function.
     */
    public final void setCustomStateType(
            final String stManagerClass,
            final String stParams) {
        this.stateType = StateType.Custom;
        this.stateManagerClass = stManagerClass;
        this.stateParams = stParams;
    }

    /**
     * @return additional state parameters.
     */
    public final String getStateParams() {
        return stateParams;
    }

    /**
     * @return the state manager class associated with the pellet.
     */
    public final String getStateManagerClass() {
        return stateManagerClass;
    }
}
