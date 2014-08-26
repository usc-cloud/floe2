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

package edu.usc.pgroup.floe.client;

import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.container.ContainerService;
import edu.usc.pgroup.floe.coordinator.CoordinatorService;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.utils.Utils;
import edu.usc.pgroup.floe.zookeeper.LocalZKServer;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import org.slf4j.LoggerFactory;

/**
 * @author kumbhare
 */
public final class AppSubmitter {

    /**
     * the global logger instance.
     */
    private static final org.slf4j.Logger LOGGER =
            LoggerFactory.getLogger(AppSubmitter.class);

    /**
     * Hiding the public constructor.
     */
    private AppSubmitter() {

    }


    /**
     * Submit the app to the Floe Coordinator.
     * @param appName name of the app.
     * @param app The floe application topology.
     * @throws TException thrift exception
     */
    public static void submitApp(final String appName, final TFloeApp app)
            throws TException {

        String mode = FloeConfig.getConfig().getString(
                ConfigProperties.FLOE_EXEC_MODE);

        if (mode.compareToIgnoreCase(Utils.Constants.LOCAL) == 0) {
            launchInProcServices();
        }

        FloeClient client = FloeClient.getInstance();

        String jar = FloeConfig.getConfig().getString(ConfigProperties
                .FLOE_EXE_JAR);

        //submit jar.
        if (jar != null) {
            String baseFile = client.uploadFileSync(jar);
            app.set_jarPath(baseFile);
        }

        //submit app
        client.submitApp(appName, app);
    }

    /**
     * Launches all inproc services.
     */
    private static void launchInProcServices() {
        Thread zkThread = new Thread(new Runnable() {
            @Override
            public void run() {
                LocalZKServer.main(new String[0]);
            }
        });

        Thread coordThread = new Thread(new Runnable() {
            @Override
            public void run() {
                CoordinatorService.main(new String[0]);
            }
        });

        Thread containerThread = new Thread(new Runnable() {
            @Override
            public void run() {
                ContainerService.main(new String[0]);
            }
        });


        try {
            zkThread.start();
            zkThread.join();

            containerThread.start();
            containerThread.join();

            coordThread.start();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        //Wait for coordinator to to return.
        while (true) {
            try {
                FloeClient client = FloeClient.getInstance();
                break;
            } catch (TTransportException e) {
                //ignore. Try again after some time.
                LOGGER.info("Coordinator not yet started.");
            }

            try {
                LOGGER.info("Waiting for coodinator to start.");
                Thread.sleep(Utils.Constants.MILLI);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * Send the shutdown request to the coordinator and exit the client.
     */
    public static void shutdown() {
        LOGGER.info("Shutting down the application.");
        //FloeClient.getInstance().shutdown();
        System.exit(0);
    }
}
