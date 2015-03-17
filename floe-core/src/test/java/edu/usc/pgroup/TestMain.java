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

package edu.usc.pgroup;

import info.ganglia.gmetric4j.gmetric.GMetric;
import info.ganglia.gmetric4j.gmetric.GangliaException;

import java.io.IOException;
import java.util.Random;

/**
 * @author kumbhare
 */
public class TestMain {
    public static void main(String[] args) {

        /**
         * Metric registry for this flake.
         */

        GMetric gmetric = null;
        try {
            final int gangliaPort = 8649;
            gmetric = new GMetric("239.2.11.71", gangliaPort,
                    GMetric.UDPAddressingMode.MULTICAST, 1, true);
        } catch (IOException e) {
            e.printStackTrace();
            //LOGGER.error("Error while initializeing ganglia client");
        }


        Random r = new Random();
        while (true) {
            if (gmetric != null) {
                try {
                    double d = r.nextDouble();
                    System.out.println("Announcing nw: " + d);
                    gmetric.announce("Test2", d, "CGROUP");
                } catch (GangliaException e) {
                    e.printStackTrace();
                    //LOGGER.error("ERROR");
                    System.exit(-1);
                }
            } else {
                System.out.println("Errr");
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}
