package edu.usc.pgroup.floe.flake.coordination;

import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.SlidingTimeWindowReservoir;
import com.codahale.metrics.Snapshot;
import edu.usc.pgroup.floe.config.ConfigProperties;
import edu.usc.pgroup.floe.config.FloeConfig;
import edu.usc.pgroup.floe.flake.FlakeToken;
import edu.usc.pgroup.floe.flake.PelletExecutor;
import edu.usc.pgroup.floe.flake.QueueLenMonitor;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.util.concurrent.TimeUnit;

/**
 * @author kumbhare
 */
public class PeriodicScaler extends ScalingLoadBalancingComponent {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PeriodicScaler.class);

    /**
     * Instance of the peer monitor class which keeps track of the neighbors.
     */
    private final PeerMonitor peerMonitor;

    /**
     * Constructor.
     *
     * @param registry      Metrics registry used to log various metrics.
     * @param flakeId       Flake's id to which this component belongs.
     * @param appName       Name of the application.
     * @param pelletName    Name of the pellet.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     * @param tracker       trackers the list of neighbor flakes.
     */
    public PeriodicScaler(final MetricRegistry registry,
                          final String flakeId,
                          final String appName,
                          final String pelletName,
                          final String componentName,
                          final ZMQ.Context ctx,
                          final PeerMonitor tracker) {
        super(registry, flakeId, appName, pelletName, componentName, ctx);
        this.peerMonitor = tracker;
    }

    /**
     * Starts all the sub parts of the given component and notifies when
     * components starts completely. This will be in a different thread,
     * so no need to worry.. block as much as you want.
     *
     * @param terminateSignalReceiver terminate signal receiver.
     */
    @Override
    protected final void runComponent(
            final ZMQ.Socket terminateSignalReceiver) {

        int scalingPeriod = FloeConfig.getConfig().getInt(ConfigProperties
                .FLAKE_STATE_CHECKPOINT_PERIOD) * Utils.Constants.MILLI;

        boolean done = false;

        ZMQ.Poller pollerItems = new ZMQ.Poller(1);
        pollerItems.register(terminateSignalReceiver, ZMQ.Poller.POLLIN);

        final int windowlen = 30; //seconds.

        Histogram qhist
                = getMetricRegistry() .register(
                MetricRegistry.name(QueueLenMonitor.class, "q.len.histo"),
                new Histogram(new SlidingTimeWindowReservoir(windowlen,
                        TimeUnit.SECONDS)));

        Meter msgProcessedMeter =  getMetricRegistry().meter(
                MetricRegistry.name(PelletExecutor.class, "processed"));

        long starttime = System.currentTimeMillis();
        final int qLenThreshold = 20;

        notifyStarted(true);

        while (!done && !Thread.currentThread().isInterrupted()) {

            int polled = pollerItems.poll(scalingPeriod);
            if (pollerItems.pollin(0)) {
                //terminate.
                LOGGER.warn("Terminating state checkpointing");
                terminateSignalReceiver.recv();
                done = true;
            }

            Snapshot snp = qhist.getSnapshot();
            //snp.dump(System.out);
            LOGGER.info("fid:{}; q 95->{}; 75->{}; 99->{}; msgs procd: {}",
                    getFid(),
                    snp.get95thPercentile(), //* durationFactor,
                    snp.get75thPercentile(), //* durationFactor,
                    snp.get99thPercentile(), //* durationFactor,
                    msgProcessedMeter.getOneMinuteRate());

            //double last1min = qhist.;
            /*LOGGER.error("fid:{}; q 1min->{}; msgs procd: {}",
                    getFid(),
                    last1min,
                    msgProcessedMeter.getOneMinuteRate());*/

            Boolean reqLB = false;
            //double a80 = (snp.get95thPercentile() * durationFactor +
            //snp.get75thPercentile() * durationFactor) / 2.0;
            double a80 = snp.get95thPercentile();

            if (stableenough(starttime)) {
                if (a80 > qLenThreshold) {
                    LOGGER.error("Initiating loadbalancing.");

                    FlakeToken neighbor = peerMonitor
                            .getNeighborsToBackupFor().get(
                                    peerMonitor.getNeighborsToBackupFor()
                                            .firstKey());

                    Integer newToken = mid(
                            peerMonitor.getFidToTokenMap().get(getFid()),
                            neighbor.getToken());

                    try {
                        scaleUp(newToken);
                    } catch (Exception e) {
                        LOGGER.error("Error while scaling up. Scaling up "
                                + "failed: {}", e);
                    }
                }
                starttime = System.currentTimeMillis();
            }


        }

        notifyStopped(true);
    }

    /**
     * @param t1 first token
     * @param t2 second token
     * @return the mid point of the two flake tokens
     */
    private Integer mid(final Integer t1, final Integer t2) {
        return t1 / 2 + t2 / 2; //FIX THIS.. this is not really mid on the
        // circle.
    }


    /**
     * Checks if enough time has passed (say a min).
     * @param starttime start time since when the time must be checked.
     * @return true if the time passed is more than a min.
     */
    private boolean stableenough(final long starttime) {
        long now = System.currentTimeMillis();
        final int secs = 30;
        final int th = 1000;
        if ((now - starttime) / th >= secs) {
            return true;
        }
        return false;
    }
}
