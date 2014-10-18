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
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Timer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * @author kumbhare
 */
public class QueueLenMonitor extends Thread {


    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(QueueLenMonitor.class);

    /**
     * Metric Registry.
     */
    private final MetricRegistry metricReg;

    /**
     * Queue counter.
     */
    private final Counter qCounter;
    private final Histogram qhist;

    /**
     * @param registry Metric registry.
     * @param counter queue counter.
     */
    public QueueLenMonitor(final MetricRegistry registry,
                           final Counter counter,
                           Histogram qhist) {
        this.metricReg = registry;
        this.qCounter = counter;
        this.qhist = qhist;
    }

    @Override
    public final void run() {

        System.out.println(metricReg.getHistograms().keySet());

        /*Meter qhist
                = metricReg.meter(MetricRegistry.name(QueueLenMonitor.class,
                "q.len.histo"));*/

        final int monitorint = 10;
        while (!Thread.interrupted()) {

            qhist.update(qCounter.getCount());
            //qhist.mark(qCounter.getCount());
            //qhist.update(qCounter.getCount(), TimeUnit.MILLISECONDS);

            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
