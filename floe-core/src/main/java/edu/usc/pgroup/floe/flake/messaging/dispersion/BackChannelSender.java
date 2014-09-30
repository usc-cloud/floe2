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

package edu.usc.pgroup.floe.flake.messaging.dispersion;

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

/**
 * @author kumbhare
 */
public class BackChannelSender extends Thread {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(BackChannelSender.class);

    /**
     * Shared zmq context.
     */
    private final ZMQ.Context ctx;

    /**
     * Shared zmq context.
     */
    private final String flakeId;

    /**
     * Name of the source pellet for this backedge.
     */
    private final String srcPellet;

    /**
     * Flake local dispersion strategy, which also decides what data should
     * be sent on the backchannel.
     */
    private final FlakeLocalDispersionStrategy dispersionStrategy;


    /**
     * Constructor.
     * @param flakeLocalDispersionStrategy the flake local strategy
     *                                     associated with this back channel.
     * @param context Shared ZMQ Context.
     * @param srcPelletName Name of the source pellet for this backedge.
     * @param fid Flake's id.
     */
    public BackChannelSender(
            final FlakeLocalDispersionStrategy flakeLocalDispersionStrategy,
            final ZMQ.Context context,
            final String srcPelletName,
            final String fid) {
        this.ctx = context;
        this.flakeId = fid;
        this.srcPellet = srcPelletName;
        this.dispersionStrategy = flakeLocalDispersionStrategy;
    }

    /**
     * Backchannel's run function.
     */
    @Override
    public final void run() {
        LOGGER.info("Open back channel from pellet");
        final ZMQ.Socket backendBackChannel = ctx.socket(ZMQ.PUB);

        backendBackChannel.connect(
                Utils.Constants.FLAKE_BACKCHANNEL_SENDER_PREFIX
                        + flakeId);


        final ZMQ.Socket backChannelPingerControl = ctx.socket(ZMQ.SUB);
        backChannelPingerControl.subscribe("".getBytes());
        backChannelPingerControl.connect(
                Utils.Constants.FLAKE_BACKCHANNEL_CONTROL_PREFIX
                        + flakeId);


        final ZMQ.Socket backChannelTimerControl = ctx.socket(ZMQ.PULL);
        backChannelTimerControl.bind(
                Utils.Constants.FLAKE_BACKCHANNEL_CONTROL_PREFIX
                        + "TIMER-" + srcPellet + "-" + flakeId);

        Thread pingger = new Thread(
          new Runnable() {
              @Override
              public void run() {
                  try {
                      ZMQ.Socket pingsock = ctx.socket(ZMQ.PUSH);
                      pingsock.connect(
                              Utils.Constants.FLAKE_BACKCHANNEL_CONTROL_PREFIX
                              + "TIMER-" + srcPellet + "-" + flakeId);

                      int sleep = FloeConfig.getConfig().getInt(
                              ConfigProperties.FLAKE_BACKCHANNEL_PERIOD);

                      final int maxMultiplier = 5;
                      int ctr = 0;
                      int multiplier = 2;

                      while (!Thread.currentThread().isInterrupted()) {
                          LOGGER.debug("BK Sender Sleeping for:" + sleep);
                          Thread.currentThread().sleep(
                                  sleep
                          );
                          if (ctr < maxMultiplier) {
                              sleep *= multiplier;
                              ctr++;
                          }
                          byte[] b = new byte[]{1};
                          pingsock.send(b, 0);
                      }
                  } catch (InterruptedException e) {
                      LOGGER.info("back channel interrupted");
                  }
              }
          }
        );

        pingger.start();


        ZMQ.Poller pollerItems = new ZMQ.Poller(2);
        pollerItems.register(backChannelPingerControl, ZMQ.Poller.POLLIN);
        pollerItems.register(backChannelTimerControl, ZMQ.Poller.POLLIN);

        while (!Thread.currentThread().interrupted()) {

            pollerItems.poll(); //receive trigger.
            byte[] pingData = new byte[]{1};
            if (pollerItems.pollin(0)) {
                pingData = backChannelPingerControl.recv();
            } else if (pollerItems.pollin(1)) { //backend
                pingData = backChannelTimerControl.recv();
            }
            byte[] data = dispersionStrategy.getCurrentBackchannelData();
            LOGGER.debug("Sending backchannel msg for {}, {}, {}.",
                    srcPellet, flakeId, data);

            String toContinue = "1";
            if (pingData[0] == 0) {
                pingger.interrupt();
                toContinue = "0";
            }

            LOGGER.debug("sending toContinue:{}", toContinue);
            backendBackChannel.sendMore(srcPellet);
            backendBackChannel.sendMore(flakeId);
            backendBackChannel.sendMore(toContinue);
            backendBackChannel.send(data, 0);
        }
    }
}
