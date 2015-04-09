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

import java.util.concurrent.Callable;

/**
 * A simple retry loop that uses the given retry policy to call the procedure.
 * @author kumbhare
 */
public final class RetryLoop {

    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(RetryLoop.class);


    /**
     * Hiding the default constructor.
     */
    private RetryLoop() {

    }

    /**
     * A simple retry loop that uses the given retry policy
     * to call the procedure.
     * @param policy RetryPolicy
     * @param callable The instance of Callable to call.
     * @param <T> Return type of the callable function.
     * @return the result of callable.call()
     * @throws java.lang.Exception Throws the exception if it is not retriable.
     */
    public static<T> T callWithRetry(final RetryPolicy policy,
                                     final Callable<T> callable)
            throws Exception {
        T result = null;
        while (policy.shouldContinue()) {
            try {
                result = callable.call();

                //If the thread reaches here, it means the function call was
                // successful. so actionCompleted and return.
                policy.actionCompleted();
                break;
            } catch (Exception e) {
                LOGGER.warn("Exception occurred while trying, "
                        + "checking exception to see if we need to retry: "
                        + "{}", e );
                //if retry attempts failed. Throw the generated exception.
                //NOTE: This will throw the last generated exception,
                // which might be different from the previous ones
                policy.checkException(e);
            }

            policy.updateCounters();

            if (policy.shouldContinue()) {
                LOGGER.warn("Retrying again after {} ms. Remaining "
                                + "attempts: {}",
                        policy.getCurrentSleepTimeMs(),
                        policy.getRemainingAttempts());
                Thread.sleep(policy.getCurrentSleepTimeMs());
            }
        }
        return  result;
    }
}
