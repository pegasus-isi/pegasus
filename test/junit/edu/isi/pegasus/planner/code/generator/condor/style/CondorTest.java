/*
 * Copyright 2007-2015 University Of Southern California
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
package edu.isi.pegasus.planner.code.generator.condor.style;

import static org.junit.Assert.*;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;
import edu.isi.pegasus.planner.namespace.Pegasus;
import org.junit.Before;
import org.junit.Test;

/**
 * To test the Condor style class for condor code generator.
 *
 * @author vahi
 */
public class CondorTest {
    private Condor cs = null;

    private static final String REQUEST_CPUS_KEY =
            edu.isi.pegasus.planner.namespace.Condor.REQUEST_CPUS_KEY;
    private static final String REQUEST_MEMORY_KEY =
            edu.isi.pegasus.planner.namespace.Condor.REQUEST_MEMORY_KEY;

    public CondorTest() {}

    @Before
    public void setUp() {
        cs = new Condor();
    }

    @Test
    public void testPegasusProfileCores() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.CORES_KEY, "5");
        testForKey(j, REQUEST_CPUS_KEY, "5");
    }

    @Test
    public void testPegasusProfileCoresAndCondorKey() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.CORES_KEY, "5");
        j.condorVariables.checkKeyInNS(REQUEST_CPUS_KEY, "6");
        testForKey(j, REQUEST_CPUS_KEY, "6");
    }

    @Test
    public void testPegasusProfileMemory() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.MEMORY_KEY, "5");
        testForKey(j, REQUEST_MEMORY_KEY, "5");
    }

    @Test
    public void testPegasusProfileMemoryAndCondorKey() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.MEMORY_KEY, "5");
        j.condorVariables.checkKeyInNS(REQUEST_MEMORY_KEY, "6");
        testForKey(j, REQUEST_MEMORY_KEY, "6");
    }

    private void testForKey(Job j, String key, String expectedValue) throws CondorStyleException {
        cs.apply(j);
        assertTrue(j.condorVariables.containsKey(key));
        assertEquals(expectedValue, j.condorVariables.get(key));
    }
}
