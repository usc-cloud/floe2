package edu.usc.pgroup.floe.flake.coordination;

import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.client.FloeClient;
import edu.usc.pgroup.floe.flake.FlakeComponent;
import edu.usc.pgroup.floe.thriftgen.ScaleDirection;
import org.apache.thrift.TException;
import org.zeromq.ZMQ;

import java.util.Arrays;
import java.util.List;


/**
 * @author kumbhare
 */
public abstract class ScalingLoadBalancingComponent extends FlakeComponent {

    /**
     * name of the application.
     */
    private final String app;

    /**
     * name of the pellet.
     */
    private final String pellet;

    /**
     * Constructor.
     *
     * @param registry      Metrics registry used to log various metrics.
     * @param flakeId       Flake's id to which this component belongs.
     * @param appName       Name of the application.
     * @param pelletName    Name of the pellet.
     * @param componentName Unique name of the component.
     * @param ctx           Shared zmq context.
     */
    public ScalingLoadBalancingComponent(final MetricRegistry registry,
                                         final String flakeId,
                                         final String appName,
                                         final String pelletName,
                                         final String componentName,
                                         final ZMQ.Context ctx) {
        super(registry, flakeId, componentName, ctx);
        this.app = appName;
        this.pellet = pelletName;
    }

    /**
     * Use this function to initiate a "scale up" function to create a new
     * flake with a given token (usually adjasant to the current flake) so as
     * to transfer the load at a later point.
     * @param token token to be used while creating the new flake (which
     *              determines it's position on the ring).
     * @throws Exception throws an excption if initiating the scale up
     * process fails.
     */
    public final void scaleUp(final Integer token) throws Exception {
        try {

            List<Integer> tokens = null;
            if (token != null) {
                tokens = Arrays.asList(token);
            }
            FloeClient.getInstance().getClient().scaleWithTokens(
                    ScaleDirection.up, app, pellet, 1, tokens
            );
        } catch (TException e) {
            throw new Exception(e);
        }
    }
}
