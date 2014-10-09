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

package edu.usc.pgroup.floe.flake.statemanager;

import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author kumbhare
 */
public class PelletState {

    /**
     * pellet instance id to which this pellet state belongs.
     */
    private final String peId;

    /**
     * A custom identifier that can be used to further identify this state's
     * owner.
     */
    private final String customId;

    /**
     * Pellet specific state object.
     */
    private HashMap<String, Object> pelletState;

    /**
     * Atomic reference that points to the currently used delta to update the
     * state.
     */
    private final AtomicReference<HashMap<String, Object>> currentDelta;


    /**
     * Atomic reference that points to the delta which is being currently
     * checkpointed and merged with the main.
     */
    private final AtomicReference<HashMap<String, Object>> currentDeltaChkpt;


    /**
     * Lock to be used while swapping.
     */
    private final Lock swapLock;

    /**
     * Lock to be used while swapping.
     */
    private final Lock mergeLock;

    /**
     * The listener object which receives notification when a state is updated.
     */
    private PelletStateUpdateListener updateListener;


    /**
     * The timestamp associated with the latest update. This is updated by
     * the pellet runner AFTER pellet execution.
     */
    private AtomicLong latestTimeStampAtomic;

    /**
     * Constructor.
     * @param peInstanceId pellet instance id to which this pellet state
     *                     belongs.
     * @param customSubId A custom identifier that can be used to further
     *                 identify this state's owner. (e.g. the reducer key)
     * @param listener The listener object which receives notification when a
     *                 state is updated.
     */
    PelletState(final String peInstanceId,
                final String customSubId,
                final PelletStateUpdateListener listener) {
        this.pelletState = new HashMap<>();
        this.updateListener = listener;
        this.peId = peInstanceId;
        this.customId = customSubId;

        this.currentDelta = new AtomicReference<>(
                new HashMap<String, Object>());

        this.currentDeltaChkpt = new AtomicReference<>(
                new HashMap<String, Object>());

        this.swapLock = new ReentrantLock();
        this.mergeLock = new ReentrantLock();
        this.latestTimeStampAtomic = new AtomicLong();
    }

    /**
     * Constructor.
     * @param peInstanceId pellet instance id to which this pellet state
     *                     belongs.
     * @param listener The listener object which receives notification when a
     *                 state is updated.
     */
    PelletState(final String peInstanceId,
                final PelletStateUpdateListener listener) {
        this(peInstanceId, null, listener);
    }

    /**
     * sets the state for the pellet.
     * Note: Always set the value in the 'deltaPelletState' object.
     * @param key the key/name for the update.
     * @param state the new state object.
     */
    public final void setValue(final String key, final Object state) {

        swapLock.lock();
        try {
            currentDelta.get().put(key, state);
        } finally {
            swapLock.unlock();
        }

        if (updateListener != null) {
            updateListener.stateUpdated(peId, customId, key, state);
        }
    }

    /**
     * Sets the timestamp for the latest update (automically).
     * @param ts time stamp.
     */
    public final void setLatestTimeStampAtomic(final long ts) {
        latestTimeStampAtomic.set(ts);
    }

    /**
     * @param key the key/name to retrieve.
     * @return the current state of the pellet.
     */
    public final Object getValue(final String key) {
        Object value;

        swapLock.lock(); //this is ok.. get and set will not be called from
        // different threads.
        try {
            value = currentDelta.get().get(key);

            if (value == null) {
                value = currentDeltaChkpt.get().get(key);
            }
        } finally {
            swapLock.unlock();
        }



        mergeLock.lock();
        try {
            if (value == null) {
                value = pelletState.get(key);
            }
        } finally {
            mergeLock.unlock();
        }
        return value;
    }

    /**
     * Starts the delta checkpointing process.
     * 1. swaps the current delta with the currentDeltaChkpt.
     * @return Returns the delta state updated since last checkpoint.
     */
    public final PelletStateDelta startDeltaCheckpointing() {
        HashMap<String, Object> current;
        HashMap<String, Object> backup;
        swapLock.lock();
        try {
            current = currentDelta.get();
            backup = currentDeltaChkpt.get();

            currentDelta.set(backup);
            currentDeltaChkpt.set(current);
        } finally {
            swapLock.unlock();
        }

        PelletStateDelta delta = new PelletStateDelta(
            latestTimeStampAtomic.get(),
            customId,
            current
        );
        return delta;
    }

    /**
     * This should be called after the delta state has been checkpointed so
     * that operations here can resume. Calling Start twice before finish is
     * not supported.
     * 1. upmerge the currentDeltaChkpt with the pellet state.
     * 2. clear the hash map associated with currentDeltaChkpt
     */
    public final void finishDeltaCheckpointing() {
        //No need for swap lock here.. since start and finish simultaneously in
        // two different threads is not supported. Need to do error checking
        // though.
        HashMap<String, Object> toMerge = currentDeltaChkpt.get();

        mergeLock.lock();
        try {
            pelletState.putAll(toMerge);
        } finally {
            mergeLock.unlock();
        }

        //again no lock required here.
        toMerge.clear();
    }

    /**
     * @return Retunrs the custom id associated with this state.
     */
    public final String getCustomId() {
        return customId;
    }
}
