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

package edu.usc.pgroup.floe.processes.cpuaffinity;

/**
 * The base affinity interface, to be implemented for each supported platform.
 * Similar to the IAffinity interface of the OpenHFT Java-Thread-Affinity
 * library.
 * https://github.com/OpenHFT/Java-Thread-Affinity
 * @author kumbhare
 */
public interface Affinity {
    /**
     * Returns the affinity mask of the process with the given pid.
     * @param pid pid of the process to get the affinity mask.
     * @return returns affinity mask for current thread, or -1 if unknown
     */
    long getAffinity(final int pid);

    /**
     * Sets the affinity for a pid given the affinity mask.
     * @param pid pid of the process to set the affinity mask.
     * @param affinity sets affinity mask of current thread to specified value
     */
    void setAffinity(final int pid, final long affinity);
}
