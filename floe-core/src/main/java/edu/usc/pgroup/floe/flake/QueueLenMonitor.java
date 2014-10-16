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

package edu.usc.pgroup.floe.flake;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.MetricRegistry;

/**
 * @author kumbhare
 */
public class QueueLenMonitor extends Thread {

    /**
     * Metric Registry.
     */
    private final MetricRegistry metricReg;

    /**
     * Queue counter.
     */
    private final Counter qCounter;

    /**
     * @param registry Metric registry.
     * @param counter queue counter.
     */
    public QueueLenMonitor(final MetricRegistry registry,
                           final Counter counter) {
        this.metricReg = registry;
        this.qCounter = counter;
    }

    @Override
    public final void run() {

        Histogram qhist
                = metricReg.histogram(MetricRegistry.name(QueueLenMonitor.class,
                                                    "q.len.histo"));

        final int monitorint = 10;
        while (!Thread.interrupted()) {

            qhist.update(qCounter.getCount());

            try {
                Thread.sleep(monitorint);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
