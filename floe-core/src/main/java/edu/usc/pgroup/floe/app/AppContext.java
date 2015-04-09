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
public class AppContext {
    /**
     * App name (as specified during application building).
     */
    private final String appName;

    /**
     * @param name App name as submitted by the user.
     */
    public AppContext(final String name) {
        this.appName = name;
    }

    /**
     * @return App name (as specified during application deployment).
     */
    public final String getAppName() {
        return appName;
    }
}
