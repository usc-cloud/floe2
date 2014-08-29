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

import edu.usc.pgroup.floe.container.FlakeControlCommand;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

/**
 * @author kumbhare
 */
public class TestZMQ {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(TestZMQ.class);

    private static class Flake {

        private final String fid;
        private final boolean source;
        private Integer[] listeningPorts;


        List<Pellet> pellets;
        ZMQ.Context context;

        public Flake(String fid, Integer[] listeningPorts, boolean source) {
            this.source = source;
            pellets = new ArrayList<>();
            this.fid = fid;
            this.listeningPorts = listeningPorts;
            this.context = ZMQ.context(3);
        }

        public void run() {
            LOGGER.info("Starting flake:"+fid);
            startSender();
            startReceiver();
        }

        void startSender() {
            new FlakeSender(context, listeningPorts).start();
        }

        void startReceiver() {
            new FlakeReceiver(context).start();
        }

        public void createPellet(String pid, boolean source) {
            Pellet p = new Pellet(context, fid, pid, source);
            p.start();
        }


        private class FlakeSender extends Thread {
            private final ZMQ.Context context;
            private final Integer[] listeningPorts;

            public FlakeSender(ZMQ.Context context, Integer[] listeningPorts) {
                this.context = context;
                this.listeningPorts = listeningPorts;
            }

            public void run() {

                new MiddleEnd().start();


                /*ZMQ.Socket middleendreceiver  = context.socket(ZMQ.SUB);
                middleendreceiver.connect("inproc://sender-middleend-"+ fid);
                middleendreceiver.subscribe("".getBytes()); //dummy topic.*/

                //ZMQ.Socket backend  = context.socket(ZMQ.PUSH);
                for(Integer port: listeningPorts) {
                    //backend.bind("tcp://*:" + port);
                    new BackEnd(port).start();
                }

                /*for(int i = 0; ; i++) {
                    backend.send(String.valueOf(i));
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }*/
                //ZMQ.proxy(middleendreceiver, backend, null);
            }

            private class BackEnd extends Thread {

                private final int port;

                public BackEnd(int port) {
                    this.port = port;
                }
                public void run() {
                    ZMQ.Socket middleendreceiver  = context.socket(ZMQ.SUB);
                    middleendreceiver.connect("inproc://sender-middleend-"+ fid);
                    middleendreceiver.subscribe("".getBytes()); //dummy topic.


                    ZMQ.Socket backend  = context.socket(ZMQ.PUSH);
                    backend.bind("tcp://*:" + port);

                    ZMQ.proxy(middleendreceiver, backend, null);
                }
            }

            private class MiddleEnd extends Thread {
                public void run() {
                    ZMQ.Socket frontend  = context.socket(ZMQ.PULL);
                    frontend.bind("inproc://sender-frontend-"+ fid);


                    ZMQ.Socket middleend  = context.socket(ZMQ.PUB);
                    middleend.bind("inproc://sender-middleend-"+ fid);

                    ZMQ.proxy(frontend, middleend, null);
                }
            }
        }

        private class FlakeReceiver extends Thread {
            private final ZMQ.Context context;

            public FlakeReceiver(ZMQ.Context context) {
                this.context = context;
            }

            public void run() {

                ZMQ.Socket frontend  = context.socket(ZMQ.PULL);

                /*for(Integer port: connectPorts) {
                    frontend.connect("tcp://localhost:" + port);
                }*/

                ZMQ.Socket backend  = context.socket(ZMQ.PUSH);
                backend.bind("inproc://receiver-backend-" + fid);


                ZMQ.Socket controlSocket = context.socket(ZMQ.REP);
                controlSocket.bind("ipc://flake-control-" + fid);

                ZMQ.Poller pollerItems = new ZMQ.Poller(2);
                pollerItems.register(frontend, ZMQ.Poller.POLLIN);
                pollerItems.register(controlSocket, ZMQ.Poller.POLLIN);

                while (!Thread.currentThread().isInterrupted()) {
                    byte[] message;
                    pollerItems.poll();
                    if (pollerItems.pollin(0)) {
                        //forward to the backend.
                        message = frontend.recv();
                        backend.send(message, 0);
                    } else if (pollerItems.pollin(1)) {
                        message = controlSocket.recv();
                        //process control message.
                        FlakeControlCommand command
                                = (FlakeControlCommand) Utils.deserialize(
                                                                    message);
                        /*FlakeCommandExecutor.execute(command);*/

                        switch (command.getCommand()) {
                            case CONNECT_PRED:
                                String connectstr = (String) command.getData();
                                LOGGER.info("Connecting to: " + connectstr);
                                frontend.connect(connectstr);
                                break;
                            case INCREMENT_PELLET:
                                String pid = (String) command.getData();
                                LOGGER.info("CREATING PELLET: " + pid + " on "
                                        + fid);
                                createPellet(pid, source);
                                break;
                            case DECREMENT_PELLET:
                                break;
                        }

                        byte result[] = new byte[1];
                        controlSocket.send(result,0);
                    }
                }
                //ZMQ.proxy(frontend, backend, null);
                /*for(int i = 0; ; i++) {
                    String recv = frontend.recvStr(Charset.defaultCharset());
                    System.out.println(fid + " " + i +" : " +recv);
                }*/

            }
        }
    }

    private static class Pellet extends Thread{
        private final String pid;
        private final String fid;
        private final boolean source;
        private final ZMQ.Context context;

        public Pellet(ZMQ.Context context, String fid, String pid,
                      boolean source) {
            this.pid = pid;
            this.fid = fid;
            this.source = source;
            this.context = context;
        }

        public void run() {
            //  Prepare our context and sockets

            ZMQ.Socket clientsender  = context.socket(ZMQ.PUSH);
            clientsender.connect("inproc://sender-frontend-"+ fid);

            ZMQ.Socket clientreceiver = null;
            clientreceiver = context.socket(ZMQ.PULL);
            clientreceiver.connect("inproc://receiver-backend-" + fid);

            int i = 0;
            while(true) {
                String data = String.valueOf(i++);
                if(!source) {
                    data = clientreceiver.recvStr(Charset.defaultCharset());
                    System.out.println(fid + " : " + pid + " : " + data);
                }

                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                clientsender.send("from:" + pid + "=" + data);
            }
            //  Send request, get reply
        }
    }

    public static void main(String[] args) {


        //NUMBER OF LISTENING PORTS IS FIXED BY THE TOPOLOGY. ONE FOR EACH
        // LOGICAL EDGE.

        Flake c1f1 = new Flake("f1", new Integer[]{5009}, true);

        Flake c1f2 = new Flake("f2", new Integer[]{5011}, false);


        /*Flake c2f4 = new Flake("f4", new Integer[]{5009, 5010},
                                     new Integer[]{5009, 5010});*/


        c1f1.run();
        c1f2.run();

        //"ipc://flake-control-" + fid
        ZMQ.Context ctx = ZMQ.context(1);

        ZMQ.Socket f1control = ctx.socket(ZMQ.REQ);
        f1control.connect("ipc://flake-control-f1");

        ZMQ.Socket f2control = ctx.socket(ZMQ.REQ);
        f2control.connect("ipc://flake-control-f2");

        byte[] results;

        //command 1 : increment pellet f1
        FlakeControlCommand command = new FlakeControlCommand(
                FlakeControlCommand.Command.INCREMENT_PELLET, "p1");
        f1control.send(Utils.serialize(command), 0);
        results = f1control.recv();


        //command 2 : increment pellet f2
        FlakeControlCommand command2 = new FlakeControlCommand(
                FlakeControlCommand.Command.INCREMENT_PELLET, "p2");
        f2control.send(Utils.serialize(command2), 0);
        results = f2control.recv();

        //command 3 : connect f2 to f1
        FlakeControlCommand command3 = new FlakeControlCommand(
                FlakeControlCommand.Command.CONNECT_PRED,
                "tcp://localhost:5009");
        f2control.send(Utils.serialize(command3), 0);
        results = f2control.recv();

        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //command 4 : increment pellet f2
        FlakeControlCommand command4 = new FlakeControlCommand(
                FlakeControlCommand.Command.INCREMENT_PELLET, "p3");
        f2control.send(Utils.serialize(command4), 0);
        results = f2control.recv();

        /*Flake c2f3 = new Flake("f3", new Integer[]{5012},
                new Integer[]{5010});
        c2f3.run();
        c2f3.createPellet("p3", false);
        c2f3.createPellet("p4", false);*/
        //c2f4.createPellet("p4", false);
    }
}
