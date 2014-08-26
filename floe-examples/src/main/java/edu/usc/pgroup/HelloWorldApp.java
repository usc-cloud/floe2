package edu.usc.pgroup;

import edu.usc.pgroup.floe.app.ApplicationBuilder;
import edu.usc.pgroup.floe.client.AppSubmitter;
import edu.usc.pgroup.floe.utils.Utils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Hello world!
 *
 */
public final class HelloWorldApp {

    /**
     * Time for which to run the application.
     */
    private static final int APP_RUNNING_TIME = 100;

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(HelloWorldApp.class);

    /**
     * Hiding the public constructor.
     */
    private HelloWorldApp() {

    }

    /**
     * Sample main.
     * @param args commandline args.
     */
    public static void main(final String[] args) {
        System.out.println("Hello World!");
        ApplicationBuilder builder = new ApplicationBuilder();

        builder.addPellet("word", new WordPellet());
        builder.addPellet("print", new PrintPellet("Prefix:"))
                .subscribe("word");

        try {
            AppSubmitter.submitApp("helloworld", builder.generateApp());
        } catch (TException e) {
           LOGGER.error("Error while deploying app. Exception {}", e);
        }


        try {
            Thread.sleep(APP_RUNNING_TIME * Utils.Constants.MILLI);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        AppSubmitter.shutdown();
    }
}
