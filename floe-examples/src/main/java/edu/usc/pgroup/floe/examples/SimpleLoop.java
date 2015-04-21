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

package edu.usc.pgroup.floe.examples;

import edu.usc.pgroup.floe.app.AppContext;
import edu.usc.pgroup.floe.app.ApplicationBuilder;
import edu.usc.pgroup.floe.app.Emitter;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.app.pellets.IteratorPellet;
import edu.usc.pgroup.floe.app.pellets.Pellet;
import edu.usc.pgroup.floe.app.pellets.PelletConfiguration;
import edu.usc.pgroup.floe.app.pellets.PelletContext;
import edu.usc.pgroup.floe.app.pellets.StatelessPellet;
import edu.usc.pgroup.floe.client.AppSubmitter;
import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.flake.statemanager.PelletState;
import edu.usc.pgroup.floe.thriftgen.TChannel;
import edu.usc.pgroup.floe.thriftgen.TChannelType;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.utils.Utils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author kumbhare
 */
public final class SimpleLoop {
    /**
     * Time for which to run the application.
     */
    private static final int APP_RUNNING_TIME = 100;

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SimpleLinear.class);



    /**
     * Hiding the public constructor.
     */
    private SimpleLoop() {

    }


    /**
     * A simple send and print pellet which prints the received value,
     * increments it and forwards it to the next pellet.
     */
    static class SendAndPrint extends StatelessPellet {

        int control = 0;
        private PelletContext pctx;

        /**
         * The setup function is called once to let the pellet initialize.
         * @param appContext Application's context. Some data related to
         *                   application's deployment.
         * @param pelletContext Pellet instance context. Related to this
         *                      particular pellet instance.
         */
        @Override
        public void onStart(final AppContext appContext,
                          final PelletContext pelletContext) {
            this.pctx = pelletContext;
            pctx.startFlakeTracker();
        }

        /**
         * Should use the conf object to configure different pellet
         * configuration.
         *
         * @param conf pellet configuration.
         */
        @Override
        protected void configureStateLessPellet(
                final PelletConfiguration conf) {

        }

        /**
         * The execute method which is called for each tuple.
         *
         * @param t       input tuple received from the preceding pellet.
         * @param emitter An output emitter
         *                which may be used by the user to emmit
         */
        @Override
        public void execute(final Tuple t, final Emitter emitter) {
            Tuple ot = new Tuple();
            if (t != null) {
                Integer type = (Integer) t.get("type");
                if (type == 0) {
                    //send control message.
                    LOGGER.error("Received data",t);
                    ot.put("type", 1);
                    ot.put("from", pctx.getPelletInstanceId());
                    emitter.emit(ot);
                } else if (type == 1) {
                    control++;
                    LOGGER.error("Received control:{}; cnt:{}, expecting:{}",
                            t, control, pctx.getCurrentFlakeList().size());
                    if (control==pctx.getCurrentFlakeList().size()) {
                        LOGGER.error("barrier done");
                        control=0;
                    }
                } else {
                    LOGGER.error("Received UNKNOWN: {}, {}", t.get("type"),
                            type);
                }
            }
        }

        /**
         * The teardown function, called when the topology is killed.
         * Or when the Pellet instance is scaled down.
         */
        @Override
        public final void teardown() {

        }

        /**
         * @return The names of the streams to be used later during emitting
         * messages.
         */
        @Override
        public List<String> getOutputStreamNames() {
            return null;
        }
    }

    /**
     * Sample main.
     * @param args commandline args.
     */
    public static void main(final String[] args) {

        System.out.println("Testing Loop!");

        ApplicationBuilder builder = new ApplicationBuilder();

        TChannel channel = new TChannel(TChannelType.CUSTOM,
        "edu.usc.pgroup.floe.flake.messaging.dispersion.BroadCastDispersion",
        "edu.usc.pgroup.floe.flake.messaging.dispersion.roundrobin.RRFlakeLocalDispersionStrategy");


        builder.addPellet("word", new TestSendPellet());
        builder.addPellet("loop", new SendAndPrint()).setParallelism(2 + 1)
                .subscribe("word")
                .subscribe("loop", channel);

        TFloeApp app = builder.generateApp();

        LOGGER.info("word edges:{}", app.get_pellets().get("loop")
                .get_outgoingEdgesWithSubscribedStreams());

        try {
            AppSubmitter.submitApp("loopapp", app);
        } catch (TException e) {
            LOGGER.error("Error while deploying app. Exception {}", e);
        }

        if (FloeConfig.getConfig().getString(ConfigProperties.FLOE_EXEC_MODE)
                .equalsIgnoreCase("local")) {
            try {
                Thread.sleep(APP_RUNNING_TIME * Utils.Constants.MILLI);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            AppSubmitter.shutdown();
        }
    }

    private static class TestSendPellet extends Pellet {
        /**
         * The execute method which is called for each tuple.
         *
         * @param t       input tuple received from the preceding pellet.
         * @param emitter An output emitter which may be used by the user to emmit
         *                results.
         * @param state   state associated with the current execution of the pellet.
         */
        @Override
        public void execute(Tuple t, Emitter emitter, PelletState state) {

            try {
                Thread.sleep(Utils.Constants.MILLI * 2 * 2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            Tuple o = new Tuple();
            o.put("type",0);
            emitter.emit(o);
            emitter.emit(o);
            emitter.emit(o);
            try {
                Thread.sleep(Utils.Constants.MILLI * 2 * 2 * 2);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        /**
         * Use to configure different aspects of the pellet,such as state type etc.
         *
         * @param pconf pellet configurer
         */
        @Override
        public void configure(PelletConfiguration pconf) {
            pconf.markAsSourcePellet();
        }

        /**
         * The setup function is called once to let the pellet initialize.
         *
         * @param appContext    Application's context. Some data related to
         *                      application's deployment.
         * @param pelletContext Pellet instance context. Related to this
         */
        @Override
        public void onStart(AppContext appContext, PelletContext pelletContext) {

        }

        /**
         * The teardown function, called when the topology is killed.
         * Or when the Pellet instance is scaled down.
         */
        @Override
        public void teardown() {

        }

        /**
         * @return The names of the streams to be used later during emitting
         * messages.
         */
        @Override
        public List<String> getOutputStreamNames() {
            return null;
        }
    }
}
