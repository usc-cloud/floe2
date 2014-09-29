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

package edu.usc.pgroup.floe.flake.messaging.dispersion.elasticreducer;

import com.google.common.hash.HashCode;
import com.google.common.hash.Hashing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author kumbhare
 */
public class Murmur32 implements HashingFunction {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ElasticReducerDispersion.class);

    /**
     * The hashing function.
     */
    private final com.google.common.hash.HashFunction hash;

    /**
     * Constructor.
     */
    public Murmur32() {
        hash = Hashing.murmur3_32();
    }

    /**
     * @param data byte serialized data.
     * @return Returns a 32 bit integer hash.
     */
    @Override
    public final int hash(final byte[] data) {
        HashCode code = hash.hashBytes(data);
        return code.asInt();
    }
}
