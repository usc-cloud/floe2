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

package edu.usc.pgroup.floe.app;

/**
 * @author kumbhare
 */
public abstract class ReducerPellet extends StatefulPellet {

    /**
     * Key field name to be used for grouping tuples.
     */
    private final String keyFieldName;

    /**
     * Constructor.
     * @param keyName name of the field from the input tuple to be
     *                     used as the key for grouping tuples.
     */
    public ReducerPellet(final String keyName) {
        this.keyFieldName = keyName;
    }

    /**
     * @return The field name which is to be used for grouping tuples.
     */
    public final String getKeyFieldName() {
        return keyFieldName;
    }
}
