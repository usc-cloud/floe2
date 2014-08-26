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

package edu.usc.pgroup.floe.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kumbhare
 */
public class RetryNPolicy implements RetryPolicy {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RetryNPolicy.class);

    /**
     * Default retry tries.
     */
    private static final int DEFAULT_RETRY_TRIES = 3;

    /**
     * Default retry timeout.
     */
    private static final long DEFAULT_TIMEOUT = 2000;

    /**
     * Max number of retry attempts.
     */
    private int maxRetries;

    /**
     * The time to sleep between the retry attempts.
     */
    private long timeOut;

    /**
     * Remaining number of retry attempts.
     */
    private int numRetries;

    /**
     * Abstract function used to initialize the specific implementation of
     * the class.
     *
     * @param args a list of arguments. Note: No check is made at compile
     *             time whether the list of args comply with the specific
     *             implementation of the retry policy. The caller and the
     *             callee are responsible for validating the required args list.
     *             throws java.lang.IllegalArgumentException If the args list
     *             is not as expected.
     */
    @Override
    public final void init(final String... args) {
        if (args.length < 2) {
            LOGGER.warn("Arguments not specified, using defaults."
                            + "MaxRetries = 3, SleepTimeBetweenRetries=2000"
            );
            maxRetries = DEFAULT_RETRY_TRIES;
            timeOut = DEFAULT_TIMEOUT;
        }

        try {
            maxRetries = Integer.parseInt(args[0]);
            timeOut = Long.parseLong(args[1]);
        } catch (Exception e) {
            LOGGER.warn("Could not get parameters from, using defaults."
                            + "MaxRetries = 3, SleepTimeBetweenRetries=2000"
            );
            maxRetries = DEFAULT_RETRY_TRIES;
            timeOut = DEFAULT_TIMEOUT;
        }
        numRetries = maxRetries;
    }

    /**
     * Abstract function to let the specific implementation decide if to
     * continue retrying.
     *
     * @return true, if retry again. false if the retry attempts have exhausted.
     */
    @Override
    public final boolean shouldContinue() {
        if (numRetries > 0) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Abstract function to notify the specific implementation that the
     * action has completed successfully.
     */
    @Override
    public final void actionCompleted() {
        //reset the retry counter so that the same policy object may be reused.
        numRetries = maxRetries;
    }

    /**
     * Abstract function to let the specific implementation decide whether
     * the exception is a retriable exception.
     *
     * @param e The exception that occured during execution of the action.
     * @throws Exception Rethrows the exception if it is not
     *                   retriable.
     */
    @Override
    public final void checkException(final Exception e) throws Exception {
        //Default behaviour is to retry on all Exceptions. Hence do not rethrow.
        //Nothing to do.
    }

    /**
     * Abstract function to let the specific implementation update it's
     * counters to be used in the next retry attempt.
     */
    @Override
    public final void updateCounters() {
        numRetries--;
    }

    /**
     * @return the number of remaining attempts.
     */
    @Override
    public final int getRemainingAttempts() {
        return numRetries;
    }

    /**
     * @return the current timeout as decided by the policy.
     */
    @Override
    public final long getCurrentSleepTimeMs() {
        return timeOut;
    }
}
