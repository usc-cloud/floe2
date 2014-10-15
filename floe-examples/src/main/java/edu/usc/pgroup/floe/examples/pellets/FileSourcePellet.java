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

package edu.usc.pgroup.floe.examples.pellets;

import edu.usc.pgroup.floe.app.AppContext;
import edu.usc.pgroup.floe.app.Emitter;
import edu.usc.pgroup.floe.app.PelletContext;
import edu.usc.pgroup.floe.app.StatelessPellet;
import edu.usc.pgroup.floe.app.Tuple;
import edu.usc.pgroup.floe.utils.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.List;
import java.util.Random;
import java.util.StringTokenizer;

/**
 * @author kumbhare
 */
public class FileSourcePellet extends StatelessPellet {

    /**
     * the global logger instance.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(FileSourcePellet.class);

    /**
     * Path to the file to be used as source.
     */
    private final String path;

    /**
     * Interval between emitting words.
     */
    private final long interval;

    /**
     * Constructor.
     * @param filePath file path.
     * @param sleepTime interval between reading lines.
     */
    public FileSourcePellet(final String filePath, final long sleepTime) {
        this.path = filePath;
        this.interval = sleepTime;
    }


    /**
     * Constructor.
     * @param filePath file path.
     */
    public FileSourcePellet(final String filePath) {
        this(filePath, Utils.Constants.MILLI);
    }

    /**
     * The execute method which is called for each tuple. (stateless)
     *
     * @param t       input tuple received from the preceding pellet.
     * @param emitter An output emitter which may be used by the user to emmit
     */
    @Override
    public final void execute(final Tuple t, final Emitter emitter) {
        LOGGER.info("Executing file source pellet.");

        RandomAccessFile aFile = null;
        MappedByteBuffer buffer = null;
        byte[] bytebuffer = null;
        try {
            LOGGER.info("Reading file.");
            aFile = new RandomAccessFile(path, "r");

            FileChannel inChannel = aFile.getChannel();
            buffer = inChannel.map(
                    FileChannel.MapMode.READ_ONLY,
                    0,
                    inChannel.size());
            //buffer.load();
            bytebuffer = new byte[(int) inChannel.size()];
            buffer.get(bytebuffer);
            aFile.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        BufferedReader in = new BufferedReader(
                new InputStreamReader(new ByteArrayInputStream(bytebuffer)));

        Random r = new Random(System.nanoTime());

        assert bytebuffer != null;

        int startpos = r.nextInt(bytebuffer.length);

        try {
            in.skip(startpos);
            in.mark(bytebuffer.length - startpos);
        } catch (IOException e) {
            e.printStackTrace();
        }

        while (true) {

            try {
                String line = in.readLine();

                if (line == null) {
                    in.reset();
                }



                StringTokenizer tokenizer = new StringTokenizer(line);

                while (tokenizer.hasMoreElements()) {
                    String token = tokenizer.nextToken();

                    Tuple ot = new Tuple();
                    ot.put("word", token);
                    LOGGER.debug("Emmitting: {}", ot);
                    emitter.emit(ot);
                }

                Thread.sleep(interval);

            } catch (InterruptedException e) {
                LOGGER.error("Exception: {}", e);
                break;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }



    /**
     * The setup function is called once to let the pellet initialize.
     *
     * @param appContext    Application's context. Some data related to
     *                      application's deployment.
     * @param pelletContext Pellet instance context. Related to this
     */
    @Override
    public final void setup(final AppContext appContext,
                            final PelletContext pelletContext) {

    }

    /**
     * The onStart function is called once just before executing the pellet
     * and after the setup function. Typically, this is used by a data source
     * pellet which does not depend on external data source but generates
     * tuples on its own.
     *
     * @param emitter An output emitter which may be used by the user to emmit
     *                results.
     */
    @Override
    public void onStart(final Emitter emitter) {

    }

    /**
     * The teardown function, called when the topology is killed.
     * Or when the Pellet instance is scaled down.
     */
    @Override
    public void teardown() {

    }

    /**
     * @return The names of the streams to be used later during emitting
     * messages.
     */
    @Override
    public final List<String> getOutputStreamNames() {
        return null;
    }
}
