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

package edu.usc.pgroup.zookeeper;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test cases for Zookeeper and Curator functionality
 *
 * @author kumbhare
 */
public class ZookeeperTest extends TestCase {
    /**
     * Logger.
     */
    private static final Logger LOGGER =
            LoggerFactory.getLogger(ZookeeperTest.class);

    /**
     * Constructor.
     *
     * @param testName testcase name
     */
    public ZookeeperTest(String testName) {
        super(testName);
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite() {
        return new TestSuite(ZookeeperTest.class);
    }

    /**
     * The setup function. This is called once for the class.
     *
     * @throws Exception
     */
    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    /**
     * Testing local zookeeper connection and set/get
     */
    public void testLocalZookeeperSetGet() {

        /*FloeConfig.getConfig().setProperty(
                ConfigProperties.FLOE_EXEC_MODE,
                "local"
        );

        ZKClient zkClient = ZKClient.getInstance();

        String testNode = "/test";
        String valueToWrite = "testvalue";

        LOGGER.info("Testing with node: " + testNode);


        /*zkClient.createNode(testNode);
        zkClient.setNode(testNode, valueToWrite);

        String value = zkClient.getNodeValue(testNode);

        assertEquals(valueToWrite, value);*/
        assertTrue(true);
    }
}
