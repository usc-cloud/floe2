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

/**
 * The retry policy class used across the project to retry certain activities
 * after a timeout.
 * @author kumbhare
 */
public interface RetryPolicy {

    /**
     * Abstract function used to initialize the specific implementation of
     * the class.
     * @param args a list of arguments. Note: No check is made at compile
     *             time whether the list of args comply with the specific
     *             implementation of the retry policy. The caller and the
     *             callee are responsible for validating the required args list.
     *             throws java.lang.IllegalArgumentException If the args list
     *             is not as expected.
     */
    void init(String... args);

    /**
     * Abstract function to let the specific implementation decide if to
     * continue retrying.
     * @return true, if retry again. false if the retry attempts have exhausted.
     */
    boolean shouldContinue();

    /**
     * Abstract function to notify the specific implementation that the
     * action has completed successfully.
     */
    void actionCompleted();

    /**
     * Abstract function to let the specific implementation decide whether
     * the exception is a retriable exception.
     * @param e The exception that occured during execution of the action.
     * @throws java.lang.Exception Rethrows the exception if it is not
     * retriable.
     */
    void checkException(Exception e) throws Exception;


    /**
     * Abstract function to let the specific implementation update it's
     * counters to be used in the next retry attempt.
     */
    void updateCounters();

    /**
     * @return the number of remaining attempts.
     */
    int getRemainingAttempts();

    /**
     * @return the current timeout as decided by the policy.
     */
    long getCurrentSleepTimeMs();
}
