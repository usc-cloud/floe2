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

import com.codahale.metrics.Gauge;
import com.codahale.metrics.Meter;
import com.codahale.metrics.RatioGauge;
import com.codahale.metrics.Timer;

/**
 * @author kumbhare
 */
public class StateSizeMonitor extends RatioGauge {

    private final Meter totalStateSize;
    private final Meter incrementStateSize;

    public StateSizeMonitor(Meter totalStateSize, Meter incrementStateSize) {
        this.totalStateSize = totalStateSize;
        this.incrementStateSize = incrementStateSize;
    }

    @Override
    protected Ratio getRatio() {
        return RatioGauge.Ratio.of(incrementStateSize.getOneMinuteRate(),
                totalStateSize.getOneMinuteRate());
    }
}
