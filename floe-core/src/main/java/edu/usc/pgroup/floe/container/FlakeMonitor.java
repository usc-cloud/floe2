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
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.HashMap;
import java.util.Map;

/**
 * @author kumbhare
 */
public final class FlakeMonitor {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FlakeMonitor.class);

    /**
     * private singleton instance.
     */
    private static FlakeMonitor instance;

    /**
     * Flake Monitor thread.
     */
    private Monitor monitor;

    /**
     * Map containing all the flakes running on the container.
     * Map from Pellet name to FlakeInfo (NOT FID).
     */
    private Map<String, FlakeInfo> flakeMap;

    /**
     * Container id.
     */
    private String containerId;


    /**
     * hiding the default constructor.
     */
    private FlakeMonitor() {
        flakeMap = new HashMap<>();
    }

    /**
     * @return singleton instance
     */
    public static synchronized FlakeMonitor getInstance() {
        if (instance == null) {
            instance = new FlakeMonitor();
        }
        return instance;
    }

    /**
     * Initialize the flake monitor.
     * @param cid container id.
     */
    public void initialize(final String cid) {
        this.containerId = cid;
    }

    /**
     * Starts the flake monitor.
     */
    public void startMonitor() {
        //currently we use ZMQ IPC to log heartbeats.
        monitor = new Monitor();
        monitor.start();
    }

    /**
     * Updates the internal flake info map.
     * @param finfo the flake info object sent by the heartbeat.
     */
    private synchronized void updateFlakeHB(final FlakeInfo finfo) {
        if (!finfo.isTerminated()) {
            flakeMap.put(finfo.getPelletId(), finfo);
        } else {
            flakeMap.remove(finfo.getPelletId());
        }
    }

    /**
     * Returns the flakeinfo object sent by the flake's heartbeat. This is a
     * threadsafe function.
     * @param fid flake id
     * @return the latest flake info object sent by the flake's hearbeat.
     * @throws FlakeNotFoundException if the given flakeid does not exist
     * or the flake has not sent its heartbeat yet.
     */
    public synchronized FlakeInfo getFlakeInfo(final String fid)
            throws FlakeNotFoundException {
        FlakeInfo info = flakeMap.get(fid);
        if (info == null) {
            LOGGER.warn("Flake: {} does not exist or has not sent a heartbeat"
                    + " yet.", fid);
            throw new FlakeNotFoundException(fid);
        }
        return info;
    }


    /**
     * @return a map from PelletName to flakeInfo objects for each flake
     * running on this container.
     */
    public Map<String, FlakeInfo> getFlakes() {
        return flakeMap;
    }

    /**
     * Internal monitor class to listen for heartbeats from flakes.
     */
    private class Monitor extends Thread {
        /**
         * The thread's run method which listens for and updates the local
         * data.
         */
        public void run() {
            ZMQ.Context ctx = ZMQ.context(1);
            final ZMQ.Socket heartBeatSoc = ctx.socket(ZMQ.PULL);
            String hbBindStr = Utils.Constants.FLAKE_HEARBEAT_SOCK_PREFIX
                    + containerId;
            LOGGER.info("Waiting for hb at: {}", hbBindStr);
            heartBeatSoc.bind(hbBindStr);

            Runtime.getRuntime().addShutdownHook(new Thread(
                new Runnable() {
                    @Override
                    public void run() {
                        LOGGER.info("Closing hb socket and shutting down");
                        heartBeatSoc.close();
                    }
                }
            ));

            while (!Thread.currentThread().isInterrupted()) {
                byte[] hb = heartBeatSoc.recv();
                FlakeInfo finfo = (FlakeInfo) Utils.deserialize(hb);
                LOGGER.debug("Received hb from:{}", finfo.getFlakeId());
                if (finfo.isTerminated()) {
                    LOGGER.info("Flake {} terminated.", finfo.getFlakeId());
                }
                updateFlakeHB(finfo);
            }

            //heartBeatSoc.close();
            LOGGER.info("Closing heartBeatSoc.");
        }
    }
}
