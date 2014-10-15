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

import edu.usc.pgroup.floe.flake.FlakeInfo;
import edu.usc.pgroup.floe.zookeeper.ZKClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.TimerTask;

/**
 * The timer task for container heartbeat.
 *
 * @author kumbhare
 */
public class ContainerHeartbeatTask extends TimerTask {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ContainerHeartbeatTask.class);

    /**
     * Overriding the TimerTask's run method.
     * The heartbeat is recorded in the zookeeper.
     */
    @Override
    public final void run() {
        ContainerInfo cinfo = ContainerInfo.getInstance();
        cinfo.updateUptime();

        LOGGER.debug("Getting flakes: ");

        Map<String, FlakeInfo> currentFlakes
                = FlakeMonitor.getInstance().getFlakes();
        if (currentFlakes != null) {
            cinfo.updateFlakes(currentFlakes);
        }

        LOGGER.debug("Sending heartbeat {}", cinfo);

        //TODO: Update other container information here.
        ZKClient.getInstance().sendContainerHeartBeat(cinfo);
    }
}
