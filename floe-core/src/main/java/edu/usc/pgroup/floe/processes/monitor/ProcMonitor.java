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

package edu.usc.pgroup.floe.processes.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Monitors a process launched using the ProcessBuilder interface.
 * A pListener "exit" is called on the ProcStatusListener interface if the
 * monitored process exits.
 * @author kumbhare
 */
public class ProcMonitor implements Runnable {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(MonitoredProc.class);

    /**
     * Reference to the monitored process.
     */
    private final Process proc;

    /**
     * A pListener listener.
     */
    private final ProcStatusListener pListener;

    /**
     * ProMonitor constructor.
     * @param p A process handler (typically returned by the ProcessBuilder).
     * @param statusListener Implementation of the ProcStatusListener interface.
     */
    public ProcMonitor(final Process p,
                       final ProcStatusListener statusListener) {
        //start a new thread.
        this.proc = p;
        this.pListener = statusListener;
        Thread t = new Thread(this);
        t.start();
    }

    /**
     * The monitor thread.
     * This is done in  separate thread. It waits for the process to end and
     * calls exited callback function.
     */
    @Override
    public final void run() {
        int exitValue = -1;
        try {
            proc.waitFor();
        } catch (InterruptedException e) {
            LOGGER.warn("The process Monitor was interrupted: Exception {}.",
                    e);
        } finally {
            //call the pListener function.
            exitValue = proc.exitValue();
            if (pListener != null) {
                pListener.exited(exitValue);
            }
        }
    }
}
