/*
 * Copyright 2007-2014 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.cluster;

import static org.junit.Assert.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test class to test runtime clustering
 *
 * @author Rajiv Mayani
 */
public class RuntimeClusteringTest {

    private TestSetup mTestSetup;
    private LogManager mLogger;
    private Horizontal mCluster;
    private Method mBestFitMethod;

    public RuntimeClusteringTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    @Before
    public void setUp() throws NoSuchMethodException {
        mTestSetup = new DefaultTestSetup();
        mCluster = new Horizontal();

        Class[] parameters = new Class[2];
        parameters[0] = List.class;
        parameters[1] = int.class;

        mBestFitMethod = mCluster.getClass().getDeclaredMethod("bestFitBinPack", parameters);
        mBestFitMethod.setAccessible(true);
    }

    @Test
    public void testClusterNum() throws IllegalAccessException, InvocationTargetException {
        int jobCount = 10;
        int clusterCount = 3;
        List<Job> jobs = new LinkedList<Job>();

        for (int i = jobCount; i > 0; --i) {
            Job j = new Job();
            j.setName(i + "");
            j.vdsNS.construct(Pegasus.RUNTIME_KEY, (i * 10) + "");

            jobs.add(j);
        }
        List<List<Job>> results = null;

        results = (List<List<Job>>) mBestFitMethod.invoke(mCluster, jobs, clusterCount);
        assertEquals(clusterCount, results.size());

        clusterCount = jobCount + 1;
        results = (List<List<Job>>) mBestFitMethod.invoke(mCluster, jobs, clusterCount);
        assertEquals(jobs.size(), results.size());
        assertEquals(jobs.size(), results.size());
    }

    @After
    public void tearDown() {
        mLogger = null;
        mTestSetup = null;
        mCluster = null;
        mBestFitMethod = null;
    }
}
