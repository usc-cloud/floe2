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

package edu.usc.pgroup.floe.processes.cpuaffinity.impl;

import com.sun.jna.LastErrorException;
import com.sun.jna.Library;
import com.sun.jna.Native;
import com.sun.jna.Platform;
import com.sun.jna.PointerType;
import com.sun.jna.ptr.IntByReference;
import com.sun.jna.ptr.LongByReference;
import edu.usc.pgroup.floe.processes.cpuaffinity.Affinity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of {@link Affinity} for Posix.
 * sched_setaffinity(3)/sched_getaffinity(3) from 'c' library.
 * Applicable to most linux/unix platforms.
 * See: {https://github.com/OpenHFT/Java-Thread-Affinity}
 * @author kumbhare
 */
public enum  PosixJNAAffinity implements Affinity {
    /**
     * Singleton instance of the PosixJNAAffinity Class.
     * TODO: See if we should use this for others too.
     */
    INSTANCE;

    /**
     * Library name to be loaded (for windows or nix).
     */
    private static final String LIBRARY_NAME =
            Platform.isWindows() ? "msvcrt" : "c";

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(PosixJNAAffinity.class);


    /**
     * true if the library using JNA was loaded correctly, false otherwise.
     */
    public static final boolean LOADED;

    /**
     * Byte size. to be used while calculating the size parameter to get/set
     * affinity.
     */
    private static final int BYTE_SIZE = 8;

    /**
     * Default Mask for LONG.
     */
    private static final long LONG_MASK = 0xFFFFFFFFL;

    /**
     * MAX cores for long mask.
     */
    private static final long LONG_MAX_CORES = 64;

    /**
     * MAX cores for int mask.
     */
    private static final long INT_MAX_CORES = 32;

    /**
     * Special mask error, raised when mask of wrong size is used (depending
     * on the number of cpus in the system).
     */
    private static final int MASK_ERROR = 22;

    /**
     * Returns the affinity mask using JNA call to the clibrary.
     * @param pid pid of the process to get the affinity mask. 0 implies,
     *            current thread/process.
     * @return the affinity mask.
     */
    @Override
    public long getAffinity(final int pid) {

        final CLibrary lib = CLibrary.INSTANCE;

        //TODO: for systems with 64+ cores...
        final LongByReference cpuset = new LongByReference(0L);
        try {
            final int ret = lib.sched_getaffinity(pid,
                    Long.SIZE / BYTE_SIZE, cpuset);
            if (ret < 0) {
                throw new IllegalStateException("sched_getaffinity(("
                        + Long.SIZE / BYTE_SIZE + ") , &("
                        + cpuset + ") ) return " + ret);
            }
            return cpuset.getValue();
        } catch (LastErrorException e) {
            if (e.getErrorCode() != MASK_ERROR) {
                throw new IllegalStateException("sched_getaffinity(("
                        + Long.SIZE / BYTE_SIZE + ") , &(" + cpuset
                        + ") ) errorNo=" + e.getErrorCode(), e);
            }
        }
        final IntByReference cpuset32 = new IntByReference(0);
        try {
            final int ret = lib.sched_getaffinity(pid,
                    Integer.SIZE / BYTE_SIZE, cpuset32);

            if (ret < 0) {
                throw new IllegalStateException("sched_getaffinity(("
                        + Integer.SIZE / BYTE_SIZE + ") , &("
                        + cpuset32 + ") ) return " + ret);
            }
            return cpuset32.getValue() & LONG_MASK;
        } catch (LastErrorException e) {
            throw new IllegalStateException("sched_getaffinity(("
                    + Integer.SIZE / BYTE_SIZE + ") , &("
                    + cpuset32 + ") ) errorNo="
                    + e.getErrorCode(), e);
        }
    }

    /**
     * Tries to set the affinity mask using JNA call to the clibrary.
     * @param pid pid of the process to set the affinity mask. 0 implies,
     *            current thread/process.
     * @param affinity sets affinity mask of current thread to specified value
     */
    @Override
    public void setAffinity(final int pid, final long affinity) {
        int procs = Runtime.getRuntime().availableProcessors();
        if (procs < LONG_MAX_CORES && (affinity & ((1L << procs) - 1)) == 0) {
            throw new IllegalArgumentException("Cannot set zero affinity");
        }
        final CLibrary lib = CLibrary.INSTANCE;
        try {
            //fixme: for systems with more then 64 cores...
            final int ret = lib.sched_setaffinity(pid, Long.SIZE / BYTE_SIZE,
                    new LongByReference(affinity));

            if (ret < 0) {
                throw new IllegalStateException("sched_setaffinity(("
                        + Long.SIZE / BYTE_SIZE + ") , &(" + affinity
                        + ") ) return " + ret);
            }
        } catch (LastErrorException e) {
            if (e.getErrorCode() != MASK_ERROR
                    || (affinity & LONG_MASK) != affinity) {
                throw new IllegalStateException("sched_setaffinity(("
                        + Long.SIZE / BYTE_SIZE + ") , &(" + affinity
                        + ") ) errorNo=" + e.getErrorCode(), e);
            }
        }

        if (procs < INT_MAX_CORES && (affinity & ((1L << procs) - 1)) == 0) {
            throw new IllegalArgumentException("Cannot set zero "
                    + "affinity for 32-bit set affinity");
        }

        final IntByReference cpuset32 = new IntByReference(0);
        cpuset32.setValue((int) affinity);
        try {
            final int ret = lib.sched_setaffinity(pid,
                    Integer.SIZE / BYTE_SIZE, cpuset32);

            if (ret < 0) {
                throw new IllegalStateException("sched_setaffinity(("
                        + Integer.SIZE / BYTE_SIZE + ") , &("
                        + Integer.toHexString(cpuset32.getValue())
                        + ") ) return " + ret);
            }
        } catch (LastErrorException e) {
            throw new IllegalStateException("sched_setaffinity(("
                    + Integer.SIZE / BYTE_SIZE + ") , &("
                    + Integer.toHexString(cpuset32.getValue())
                    + ") ) errorNo=" + e.getErrorCode(), e);
        }
    }


    /**
     * @author BegemoT
     */
    interface CLibrary extends Library {
        /**
         * Pointer to the CLibrary.
         */
        CLibrary INSTANCE = (CLibrary)
                Native.loadLibrary(LIBRARY_NAME, CLibrary.class);

        /**
         * The native set affinity function.
         * @param pid if of the process/thread.
         * @param cpusetsize size of the cpuset flag (in bytes)
         * @param cpuset the cpuset mask to be used for the process.
         * @return 0 if no error, otherwise non-zero error code.
         * @throws LastErrorException wrapper for the exception from the library
         */
        int sched_setaffinity(final int pid,
                              final int cpusetsize,
                              final PointerType cpuset)
                throws LastErrorException;

        /**
         * The native get affinity function.
         * @param pid if of the process/thread.
         * @param cpusetsize size of the cpuset flag (in bytes)
         * @param cpuset the cpuset mask to be used for the process.
         * @return 0 if no error, other wise a non-zero error code.
         * @throws LastErrorException wrapper for the exception from the library
         */
        int sched_getaffinity(final int pid,
                              final int cpusetsize,
                              final PointerType cpuset)
                throws LastErrorException;


        /* CURRENTLY NOT USED.
        int sched_getcpu() throws LastErrorException;

        int getpid() throws LastErrorException;

        int syscall(int number, Object... args) throws LastErrorException;*/
    }
    static {
        boolean loaded = false;
        try {
            INSTANCE.getAffinity(0);
            loaded = true;
            LOGGER.info("Loaded the JNA library successfully.");
        } catch (UnsatisfiedLinkError e) {
            LOGGER.warn("Unable to load jna library {}", e);
        }
        LOADED = loaded;
    }
}
