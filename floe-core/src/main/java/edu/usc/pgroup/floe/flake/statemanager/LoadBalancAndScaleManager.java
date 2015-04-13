package edu.usc.pgroup.floe.flake.statemanager;

import edu.usc.pgroup.floe.utils.Utils;
import org.zeromq.ZMQ;

/**
 * @author kumbhare
 */
public class LoadBalancAndScaleManager {

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

        checkptControl.send("initiate");

        checkptControl.recv(); //wait for request to complete.

        checkptControl.close();
    }

    /**
     * Initiates the load balance process.
     */
    public final void initiateLoadBalance() {
        ZMQ.Socket loadBalanceControl = context.socket(ZMQ.REQ);
        loadBalanceControl.connect(
                Utils.Constants.LDBL_CTRL_BIND_STR + flakeId);

        loadBalanceControl.send("initiate");

        loadBalanceControl.recv(); //wait for request to complete.

        loadBalanceControl.close();
    }
}
