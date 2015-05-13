package edu.usc.pgroup.floe.app.pellets;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.flake.statemanager.StateManager;
import edu.usc.pgroup.floe.serialization.TupleSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ;

import java.nio.charset.Charset;
import java.util.Iterator;

/**
 * @author kumbhare
 */
public class TupleItertaor implements Iterator<Tuple> {

    /**
     * pelletInstanceId (has to be unique). Best
     * practice: use flakeid-intancecount on the
     * flake as the pellet id.
     */
    private final String pelletInstanceId;


    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(TupleItertaor.class);

    /**
     *  The common state manager object. This is one per
     *                     flake and common fro all pellet instances. Should
     *                     be thread safe.
     */
    private final StateManager pelletStateManager;

    /**
     * Serializer to be used to serialize and deserialize the data tuples.
     */
    private final TupleSerializer tupleSerializer;

    /**
     * zmq data receiver socket associated with the iterator.
     */
    private final ZMQ.Socket receiver;

    /**
     * Metric registry.
     */
    private final MetricRegistry registry;

    /**
     * meter to measure the rate of message read.
     */
    private final Meter msgDequeuedMeter;

    /**
     * Constructor.
     * @param peInstanceId pelletInstanceId (has to be unique). Best
     * practice: use flakeid-intancecount on the
     * flake as the pellet id.
     * @param ptStateMgr The common state manager object. This is one
     *                           per flake and common fro all pellet
     *                           instances. Should  be thread safe.
     * @param tSerializer  Serializer to be used to serialize and deserialize
 *                     the data tuples.
     * @param dataReceiver zmq data receiver socket associated with the
     * @param metricRegistry metric registry.
     */
    public TupleItertaor(final String peInstanceId,
                         final StateManager ptStateMgr,
                         final TupleSerializer tSerializer,
                         final ZMQ.Socket dataReceiver,
                         final MetricRegistry metricRegistry) {
        this.pelletInstanceId = peInstanceId;
        this.pelletStateManager = ptStateMgr;
        this.tupleSerializer = tSerializer;
        this.receiver = dataReceiver;
        this.registry = metricRegistry;

        this.msgDequeuedMeter =  metricRegistry.meter(
                MetricRegistry.name(TupleItertaor.class, "dequed"));
    }

    /**
     * @return returns true if the iterator currently has a tuple to read,
     * returns false otherwise, without blocking.
     */
    @Override
    public final boolean hasNext() {
        return receiver.hasReceiveMore();
    }

    /**
     * @return a tuple from the tuple stream if available. If no tuple is
     * available, the function will block untill a tuple is received.
     */
    @Override
    public final Tuple next() {

        receiver.recvStr(Charset.defaultCharset()); //TS.. ignore.
        /*String sentTime
                = dataReceiver.recvStr(Charset.defaultCharset());*/

        byte[] serializedTuple = receiver.recv();

        //queLen.dec();

        //long queueAddedTimeL = Long.parseLong(queueAddedTime);
        //long queueRemovedTime = System.nanoTime();
        //queueTimer.update(queueRemovedTime - queueAddedTimeL
        //        , TimeUnit.NANOSECONDS);

        //msgDequeuedMeter.mark();

        Tuple tuple = tupleSerializer.deserialize(serializedTuple);
        LOGGER.debug("Read tuple:{}", tuple);

        msgDequeuedMeter.mark();

        return tuple;
        /*if (state != null) {
            state.setLatestTimeStampAtomic(
                    Long.parseLong(sentTime));
        }*/


        //long processedTime = System.nanoTime();
        //processTimer.update(processedTime - queueRemovedTime,
        //        TimeUnit.NANOSECONDS);
        //msgProcessedMeter.mark();
    }

    @Override
    public final void remove() {
        throw new UnsupportedOperationException("Remove operation not "
                + "supported on the Tuple iterator");
    }

    /**
     * @return returns the pellet instance id assosicated with the iterator.
     */
    public final String getPeId() {
        return pelletInstanceId;
    }
}
