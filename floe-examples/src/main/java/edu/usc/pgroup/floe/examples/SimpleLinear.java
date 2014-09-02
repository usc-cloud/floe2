package edu.usc.pgroup.floe.examples;

import edu.usc.pgroup.floe.app.ApplicationBuilder;
import edu.usc.pgroup.floe.client.AppSubmitter;
import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.examples.pellets.PrintPellet;
import edu.usc.pgroup.floe.examples.pellets.WordPellet;
import edu.usc.pgroup.floe.utils.Utils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public final class SimpleLinear {

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
    private SimpleLinear() {

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
        builder.addPellet("print", new PrintPellet())
                .subscribe("word").setParallelism(2 * 2);

        try {
            AppSubmitter.submitApp("helloworld", builder.generateApp());
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
