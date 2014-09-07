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

package edu.usc.pgroup.zmq;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.nio.charset.Charset;

/**
 * @author kumbhare
 */
public class TestXPUBSUB {
    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(TestZMQ.class);


    private static final String PROXY_PREFIX = "inproc://proxy-prefix-";
    private static final String SUBBER_BIND = "tcp://*:9898";

    static class Pubber extends Thread {

        private final ZMQ.Context context;
        private final int id;

        public Pubber(ZMQ.Context ctx, int id) {
            this.context = ctx;
            this.id = id;
        }

        @Override
        public void run() {
            ZMQ.Socket pubber = context.socket(ZMQ.PUB);
            pubber.connect(PROXY_PREFIX);
            while (true) {
                LOGGER.info("sending msg: {}, {}", id, "ping");
                pubber.sendMore(String.valueOf(id));
                pubber.send("ping".getBytes(), 0);
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }


    static class Subber extends Thread {
        private final int id;

        public Subber(int id) {
            this.id = id;
        }

        @Override
        public void run() {
            ZMQ.Context context = ZMQ.context(1);
            ZMQ.Socket subber = context.socket(ZMQ.SUB);

            subber.bind(SUBBER_BIND);
            subber.subscribe("".getBytes());
            while (true) {
                String key = subber.recvStr(Charset.defaultCharset());
                byte[] message = subber.recv();
                LOGGER.info("SUBBER RECEIVED: {}, {}", key, message);
            }
        }
    }

    static class XPUBSUBProxy extends Thread {

        private final ZMQ.Context context;

        XPUBSUBProxy(ZMQ.Context ctx) {
            this.context = ctx;
        }


        @Override
        public void run() {
            ZMQ.Socket xsubFromPubber = context.socket(ZMQ.XSUB);
            xsubFromPubber.bind(PROXY_PREFIX);

            ZMQ.Socket xpubToSubber = context.socket(ZMQ.XPUB);
            xpubToSubber.connect("tcp://localhost:9898");

            //ZMQ.proxy(xsubFromPubber, xpubToSubber, null);

            ZMQ.Poller pollerItems = new ZMQ.Poller(2);
            pollerItems.register(xsubFromPubber, ZMQ.Poller.POLLIN);
            pollerItems.register(xpubToSubber, ZMQ.Poller.POLLIN);

            byte[] message;
            boolean more = false;
            while (true) {
                pollerItems.poll();
                if (pollerItems.pollin(0)) {

                    while (true) {
                        // receive message
                        message = xsubFromPubber.recv(0);
                        more = xsubFromPubber.hasReceiveMore();

                        // Broker it
                        xpubToSubber.send(message, more ? ZMQ.SNDMORE : 0);
                        if(!more){
                            break;
                        }
                    }
                } else if (pollerItems.pollin(1)) {
                    while (true) {
                        // receive message
                        message = xpubToSubber.recv(0);
                        more = xpubToSubber.hasReceiveMore();
                        // Broker it
                        xsubFromPubber.send(message, more ? ZMQ.SNDMORE : 0);
                        if(!more){
                            break;
                        }
                    }
                }
            }

        }
    }

    public static void main(String[] args) {
        ZMQ.Context context = ZMQ.context(1);

        new XPUBSUBProxy(context).start();

        int pubberCount = 3;
        for (int i = 0; i < pubberCount; i++) {
            new Pubber(context,i).start();
        }

        new Subber(0).start();
    }
}
