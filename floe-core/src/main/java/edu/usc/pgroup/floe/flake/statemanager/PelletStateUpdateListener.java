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

/**
 * @author kumbhare
 */
public interface PelletStateUpdateListener {
    /**
     * @param srcPeId pellet instance id which resulted in this update
     * @param customId A custom identifier that can be used to further
     *                 identify this state's owner.
     * @param key the key for the state update.
     * @param value the updated value.
     * NOTE: THIS HAS TO BE THREAD SAFE....
     */
    void stateUpdated(String srcPeId,
                             String customId,
                             String key,
                             Object value);
}
