package edu.usc.pgroup.floe.signals;

import java.io.Serializable;

/**
 * @author kumbhare
 */
public abstract class Signal implements Serializable {

    /**
     * Signal signalData.
     */
    private final byte[] signalData;


    /**
     * Constructs the signal with given signal data.
     * @param data serialized signal data.
     */
    protected Signal(final byte[] data) {
        this.signalData = data;
    }


    /**
     * @return the signal signalData.
     */
    public final byte[] getSignalData() {
        return signalData;
    }
}
