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

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;

/**
 * @author kumbhare
 */
public class ClassLoaderObjectInputStream extends ObjectInputStream {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ClassLoaderObjectInputStream.class);

    /**
     * Custom class loader.
     */
    private final ClassLoader classLoader;

    /**
     * Updated constructor that also takes a class loader as a param.
     * @param cl custom class loader to be used for OIS
     * @param in Object input stream.
     * @throws IOException if there is error reading the input stream.
     */
    public ClassLoaderObjectInputStream(
            final ClassLoader cl,
            final InputStream in) throws IOException {
        super(in);
        this.classLoader = cl;
    }

    /**
     * Overriding the resolveClass function to use the given class loader
     * instead of the default one.
     * @param desc Object Stream class descriptor.
     * @return a Class object returned by Class.forName
     * @throws IOException if there is an error reading the object stream.
     * @throws ClassNotFoundException if the given class is not found in the
     * class loader.
     */
    @Override
    protected final Class<?> resolveClass(final ObjectStreamClass desc)
            throws IOException, ClassNotFoundException {

        try {
            String name = desc.getName();
            LOGGER.info("Loading class: {}.", desc.getName());
            return Class.forName(name, false, classLoader);
        } catch (ClassNotFoundException e) {
            LOGGER.warn("Error Loading class: {}. Using the default class "
                            + "loader", desc.getName());
            return super.resolveClass(desc);
        }
    }
}
