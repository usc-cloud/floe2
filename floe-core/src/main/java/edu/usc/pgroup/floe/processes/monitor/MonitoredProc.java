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

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * A monitored process.
 * It launches a child process under the monitor and
 * whenever the child process is terminated, will relaunch it automatically.
 * @author kumbhare
 */
public class MonitoredProc implements ProcStatusListener {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(MonitoredProc.class);

    /**
     * Child process to launch.
     */
    private Process childProc;

    /**
     * The process command.
     */
    private String procToLaunch = null;

    /**
     * The process monitor.
     */
    private ProcMonitor monitor;

    /**
     * Constructor.
     * @param procCommand The Command to be launched and monitored.
     */
    public MonitoredProc(final String procCommand) {
        this.procToLaunch = procCommand;
    }

    /**
     * Internal function to launch and monitor the process.
     */
    protected final void launchAndMonitorProc() {
        try {
            LOGGER.info("Launching Process:" + procToLaunch);
            String[] command = procToLaunch.split("\\s+");

            ProcessBuilder builder = new ProcessBuilder(command);

            //MEDIUM PRIORITY
            //TODO: SHOULD LAUNCH A PROCESS SUCH THAT IT DOES NOT EXIT WHEN
            // THE PARENT DIES.
            //TODO: THIS ALSO MEANS THAT THE PARENT SHOULD BE ABLE TO GET
            // HOLD OF THE EXISTING PROCESS FOR MONITORING WHEN IT RELAUNCHES.
            // UPDATE: The issue is not as bad. Flake still survives. I think
            // the issue was cntrl-c

            childProc = builder.start();
            LOGGER.info("process Started");

            monitor = new ProcMonitor(childProc, this);
        } catch (IOException e) {
            e.printStackTrace();
            LOGGER.error("Error occured while launching process. Exception: {}",
                    e);
        }
    }

    /**
     * The ProcStatusListener callback implementation to relaunch the
     * terminated process.
     * @param exitcode the exit code from the terminated process.
     */
    @Override
    public final void exited(final int exitcode) {
        //launch proc. again.
        LOGGER.warn("Process exited with exit code:" + exitcode);

        int relaunchDelay = FloeConfig.getConfig().getInt(
                ConfigProperties.FLAKE_LAUNCH_DELAY);

        LOGGER.info("Launching again after a delay of {} milliseconds",
                relaunchDelay
                );

        if (relaunchDelay > 0) {
            try {
                Thread.sleep(relaunchDelay);
            } catch (InterruptedException e) {
                LOGGER.warn("Error occurred while waiting for relaunching the "
                        + "monitored process. Exception: {}", e);
            }
        }

        launchAndMonitorProc();
    }

    /**
     * @return returns the process object.
     */
    public final Process getProcess() {
        return childProc;
    }

    /**
     * starts the monitored process.
     */
    public final void start() {
        LOGGER.info("Launching process.");
        launchAndMonitorProc();
    }
}
