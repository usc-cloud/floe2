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

import com.codahale.metrics.Meter;
import com.codahale.metrics.RatioGauge;

/**
 * @author kumbhare
 */
public class StateSizeMonitor extends RatioGauge {

    /**
     * total state size.
     */
    private final Meter totalStateSize;
    /**
     * state size since last checkpoint.
     */
    private final Meter incrementStateSize;

    /**
     * constructor.
     * @param fullStateSize full state size meter.
     * @param incrStateSize incremental state size meter.
     */
    public StateSizeMonitor(final Meter fullStateSize,
                            final Meter incrStateSize) {
        this.totalStateSize = fullStateSize;
        this.incrementStateSize = incrStateSize;
    }

    /**
     * @return the last one minute rate of full to incremental state size ratio.
     */
    @Override
    protected final Ratio getRatio() {
        return RatioGauge.Ratio.of(incrementStateSize.getOneMinuteRate(),
                totalStateSize.getOneMinuteRate());
    }
}
