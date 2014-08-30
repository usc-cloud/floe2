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

package edu.usc.pgroup.floe.app;

import edu.usc.pgroup.floe.thriftgen.TAlternate;
import edu.usc.pgroup.floe.thriftgen.TChannelType;
import edu.usc.pgroup.floe.thriftgen.TEdge;
import edu.usc.pgroup.floe.thriftgen.TFloeApp;
import edu.usc.pgroup.floe.thriftgen.TPellet;
import edu.usc.pgroup.floe.utils.Utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/**
 * @author kumbhare
 */
public final class ApplicationBuilder {

    /**
     * Map from pellet name to TPellets.
     */
    private Map<String, TPellet> pellets;

    /**
     * Default constructor.
     */
    public ApplicationBuilder() {
        pellets = new HashMap<>();
    }

    /**final
     * Add a pellet to the topology.
     * @param pelletId Pellet id
     * @param p A pellet instance
     * @return A PelletBuilder to configure the pellet (e.g. subscribe to
     * streams etc.)
     */
    public PelletBuilder addPellet(final String pelletId, final Pellet p) {
        TPellet tPellet = new TPellet();
        TAlternate alternate = new TAlternate();
        alternate.set_serializedPellet(Utils.serialize(p));
        alternate.set_value(1.0);

        tPellet.set_alternates(new HashMap<String, TAlternate>());
        tPellet.get_alternates().put(Utils.Constants.DEFAULT_ALTERNATE_NAME,
                alternate);
        tPellet.set_activeAlternate(Utils.Constants.DEFAULT_ALTERNATE_NAME);

        tPellet.set_id(pelletId);
        tPellet.set_incomingEdges(new ArrayList<TEdge>());
        tPellet.set_outgoingEdges(new ArrayList<TEdge>());

        pellets.put(pelletId, tPellet);
        return new PelletBuilder(tPellet);
    }


    /**final
     * Add a dynamic pellet to the topology.
     * @param pelletId Pellet id
     * @return A DynamicPelletBuilder to configure the pellet (e.g. subscribe
     * to streams etc.) and add alternates as required.
     */
    public DynamicPelletBuilder addDynamicPellet(final String pelletId) {
        TPellet tPellet = new TPellet();

        tPellet.set_alternates(new HashMap<String, TAlternate>());

        tPellet.set_id(pelletId);
        tPellet.set_incomingEdges(new ArrayList<TEdge>());
        tPellet.set_outgoingEdges(new ArrayList<TEdge>());

        pellets.put(pelletId, tPellet);
        return new DynamicPelletBuilder(tPellet);
    }

    /**
     * Generate a TFloeApp from the current configuration.
     * @return TFloeApp instance to send to the coordinator service.
     */
    public TFloeApp generateApp() {
        TFloeApp app = new TFloeApp();
        app.set_pellets(pellets);
        return app;
    }

    /**
     * Internal PelletBuilder class using the builder pattern to configure
     * pellets.
     */
    public class PelletBuilder {

        /**
         * Underlying pellet object.
         */
        private TPellet pellet;


        /**
         * Protected default constructor for subclass (such as dynamic
         * pellets to handle TPellets as they wish).
         */
        protected PelletBuilder() {

        }

        /**
         * Constructor.
         * @param p The TPellet instance to configure.
         */
        public PelletBuilder(final TPellet p) {
            this.pellet = p;
        }

        /**
         * Subscribe to a stream.
         * @param inputPelletName    name of the preceding pellet.
         * @return The builder pattern's object to further configure the pellet.
         */
        public final PelletBuilder subscribe(final String inputPelletName) {
            TPellet inputPellet = pellets.get(inputPelletName);
            //TEdge edge = new TEdge(pellet.get_id(), TChannelType.roundrobin);
            TEdge edge = new TEdge(inputPellet.get_id(), pellet.get_id(),
                    TChannelType.roundrobin);
            pellet.get_incomingEdges().add(edge);
            inputPellet.get_outgoingEdges().add(edge);
            //inputPellet.get_incomingEdges().add(edge);
            return this;
        }


        /**
         * Set the parallelism for the pellet (across the cluster). This
         * refers to the number of pellet instances that will be created
         * during deployment.
         * @param numInstances number of PE instances.
         * @return The builder pattern's object to further configure the pellet.
         */
        public final PelletBuilder setParallelism(final int numInstances) {
            pellet.set_parallelism(numInstances);
            return this;
        }
    }


    /**
     * Dynamic Pellet Builder class using the builder pattern.
     */
    public class DynamicPelletBuilder extends PelletBuilder {

        /**
         * Underlying pellet object.
         */
        private final TPellet pellet;

        /**
         * Constructor.
         *
         * @param p The TPellet instance to configure.
         */
        public DynamicPelletBuilder(final TPellet p) {
            super(p);
            this.pellet = p;
        }


        /**
         * Adds an alternate to the given dynamic pellet.
         * @param alternateName name of the alternate. (this can be used
         *                      later at runtime to tell the coordinator to
         *                      switch the alternate)
         * @param value The relative value associated with this alternate.
         * @param p A pellet instance (implementation) for this alternate.
         * @return The builder pattern's object to further configure the pellet.
         */
        public final DynamicPelletBuilder addAlternate(
                final String alternateName,
                final Double value,
                final Pellet p) {

            TAlternate alternate = new TAlternate();
            alternate.set_serializedPellet(Utils.serialize(p));
            alternate.set_value(value);

            this.pellet.get_alternates().put(alternateName, alternate);
            return this;
        }

        /**
         * Sets the given alternate as active during deployment.
         * @param alternateName name of the alternate to mark as active.
         * @return The builder pattern's object to further configure the pellet.
         */
        public final DynamicPelletBuilder setActiveAlternate(
                final String alternateName) {
            if (this.pellet.get_alternates().containsKey(alternateName)) {
                this.pellet.set_activeAlternate(alternateName);
            } else {
                throw new IllegalArgumentException("Illegal alternate name: "
                        + alternateName + ". "
                        + "Given alternate does not exist for the pellet.");
            }
            return this;
        }
    }
}
