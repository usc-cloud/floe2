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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kumbhare
 */
public class BackupLenMonitor extends Thread {
    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(BackupLenMonitor.class);

    /**
     * backuplen gauge to monitor.
     */
    private final Gauge<Integer> lengauge;

    /**
     * The histogram of recent history of the queuelength.
     */
    private final Histogram qhist;

    /**
     * @param len backuplen gauge to monitor.
     * @param hist The histogram of recent history of the queuelength.
     */
    public BackupLenMonitor(final Gauge<Integer> len, final Histogram hist) {
        this.lengauge = len;
        this.qhist = hist;
    }

    /**
     * Monitor thread's run method.
     */
    @Override
    public final void run() {

        /*Meter qhist
                = metricReg.meter(MetricRegistry.name(QueueLenMonitor.class,
                "q.len.histo"));*/

        final int monitorint = 10;

        while (!Thread.interrupted()) {

            //LOGGER.error("bk len:" + lengauge.getValue());
            qhist.update(lengauge.getValue());

            try {
                Thread.sleep(monitorint);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
