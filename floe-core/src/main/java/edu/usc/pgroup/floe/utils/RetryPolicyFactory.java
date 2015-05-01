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
 * The generic retry policy factory class which returns the appropriate retry
 * policy based on the given class name or type.
 * Fixme: currently only supports class name.
 * @author kumbhare
 */
public final class RetryPolicyFactory {

    /**
     * Default policy.
     */
    private static final String DEFAULT_POLICY
            = "edu.usc.pgroup.floe.utils.RetryNPolicy";

    /**
     * args for the default policy.
     */
    private static final String[] DEFAULT_ARGS = {"10", "2000"};

    /**
     * @param klass name of the class.
     * @param args a list of Policy specific args.
     * @return the instance of the given retry policy.
     */
    public static RetryPolicy getRetryPolicy(final String klass,
                                             final String... args) {
        RetryPolicy policy = (RetryPolicy) Utils.instantiateObject(klass);
        if (policy != null) {
            policy.init(args);
        }
        return  policy;
    }

    /**
     * Hiding the default constructor.
     */
    private RetryPolicyFactory() {

    }

    /**
     * @return The default retry policy.
     */
    public static RetryPolicy getDefaultPolicy() {
        return getRetryPolicy(DEFAULT_POLICY, DEFAULT_ARGS);
    }
}
