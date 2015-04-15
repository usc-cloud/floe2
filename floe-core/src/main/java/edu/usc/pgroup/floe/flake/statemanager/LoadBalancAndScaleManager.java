package edu.usc.pgroup.floe.flake.statemanager;

import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.nio.charset.Charset;

/**
 * @author kumbhare
 */
public class LoadBalancAndScaleManager {


    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(LoadBalancAndScaleManager.class);

    /**
     * Shared ZMQ Context.
     */
    private final ZMQ.Context context;

    /**
     * Flake's id.
     */
    private final String flakeId;

    /**
     * Consxtructor.
     * @param fid own flake's id.
     * @param ctx zmq context used to send control signals.
     */
    public LoadBalancAndScaleManager(final String fid,
                                     final ZMQ.Context ctx) {
        this.flakeId = fid;
        this.context = ctx;
    }


    /**
     * Forces the checkpoint process right away.
     */
    public final void checkpointNow() {
        ZMQ.Socket checkptControl = context.socket(ZMQ.REQ);

        checkptControl.connect(Utils.Constants.CHKPT_CTRL_BIND_STR + flakeId);

        LOGGER.error("trying to initiate checkpoint");
        checkptControl.send("initiate");

        checkptControl.recv(); //wait for request to complete.
        LOGGER.error("checkpoint sent to neighbor "
                + "(not necessarily processed by it)");

        checkptControl.close();
    }

    /**
     * Initiates the load balance process.
     * @return true if the load balance process was initialized, false other
     * wise. (e.g. if the repartition state returns null).
     */
    public final boolean initiateLoadBalance() {
        ZMQ.Socket loadBalanceControl = context.socket(ZMQ.REQ);
        loadBalanceControl.connect(
                Utils.Constants.LDBL_CTRL_BIND_STR + flakeId);

        LOGGER.error("trying to initiate loadbalacing");
        loadBalanceControl.send("loadbalance");

        //wait for request to complete.
        String initiated = loadBalanceControl.recvStr(Charset.defaultCharset());

        LOGGER.error("Relevant keys sent to the neighbor"
                + "(not necessarily processed by it)");

        loadBalanceControl.close();
        return Boolean.parseBoolean(initiated);
    }

    /**
     * Initiates the load balance process.
     * @return true if the load balance process was initialized, false other
     * wise. (e.g. if the repartition state returns null).
     */
    public final boolean initiateScaleOut() {
        ZMQ.Socket loadBalanceControl = context.socket(ZMQ.REQ);
        loadBalanceControl.connect(
                Utils.Constants.LDBL_CTRL_BIND_STR + flakeId);

        LOGGER.error("trying to initiate scale out process.");
        loadBalanceControl.send("scale");

        //wait for request to complete.
        String initiated = loadBalanceControl.recvStr(Charset.defaultCharset());

        LOGGER.error("Scale out command has been sent to the coordinator."
                + "You can you use flake tracker to check if the flake has "
                + "been deployed.");

        loadBalanceControl.close();
        return Boolean.parseBoolean(initiated);
    }

}
