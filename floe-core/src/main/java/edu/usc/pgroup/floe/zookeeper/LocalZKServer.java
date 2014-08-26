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

package edu.usc.pgroup.floe.zookeeper;

import org.apache.curator.test.TestingServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Timer;
import java.util.TimerTask;

/**
 * Local ZK Server.
 *
 * @author kumbhare
 */
public final class LocalZKServer {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(LocalZKServer.class);

    /**
     * PORT for local ZK server.
     */
    private static final int PORT = 2181;

    /**
     * Heartbeat period.
     */
    private static final int PERIOD = 10 * 1000;

    /**
     * Hiding the default constructor.
     */
    private LocalZKServer() {

    }

    /**
     * The entry point for the ZK Test server.
     *
     * @param args commandline arguments.
     */
    public static void main(final String[] args) {
        try {
            TestingServer server = new TestingServer(
                    PORT, true
            );


            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask() {
                  @Override
                  public void run() {
                      /*LOGGER.info("ZK Server is running. LM: "
                              + Long.MAX_VALUE);
                      try {
                          List<String> children = ZKClient.getInstance()
                                  .getCuratorClient()
                                  .getChildren().
                                          forPath("/");
                          for (String child : children) {
                              LOGGER.info(child);
                          }

                      } catch (Exception e) {
                          e.printStackTrace();
                      }*/
                  }
            },
                    0, PERIOD);
        } catch (Exception e) {
            e.printStackTrace();
            LOGGER.error("Could not start the test ZK server");
            throw new RuntimeException(e);
        }
    }
}
