package edu.usc.pgroup.floe.examples;

import edu.usc.pgroup.floe.app.ApplicationBuilder;
import edu.usc.pgroup.floe.client.AppSubmitter;
import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.examples.pellets.PrintIteratorPellet;
import edu.usc.pgroup.floe.examples.pellets.PrintPellet;
import edu.usc.pgroup.floe.examples.pellets.WordPellet;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.utils.Utils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public final class SimpleInputIterator {

    /**
     * Time for which to run the application.
     */
    private static final int APP_RUNNING_TIME = 100;

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(SimpleInputIterator.class);

    /**
     * Hiding the public constructor.
     */
    private SimpleInputIterator() {

    }

    /**
     * Sample main.
     * @param args commandline args.
     */
    public static void main(final String[] args) {
        System.out.println("Hello World!");
        ApplicationBuilder builder = new ApplicationBuilder();

        String[] words = {"John", "Jane", "Maverick", "Alok"};

        builder.addPellet("word", new WordPellet(words)).setParallelism(1);

        builder.addPellet("print", new PrintIteratorPellet())
                .subscribe("word").setParallelism(2);


        /*builder.addPellet("hello", new HelloGreetingPellet())
                .subscribe("print").setParallelism(1);*/

        TFloeApp app = builder.generateApp();
        /*LOGGER.info("word edges:{}", app.get_pellets().get("word")
                .get_outgoingEdgesWithSubscribedStreams());

        LOGGER.info("print edges:{}", app.get_pellets().get("print")
                .get_outgoingEdgesWithSubscribedStreams());*/




        try {
            AppSubmitter.submitApp("helloworld", app);
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
}
