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
import edu.isi.pegasus.planner.namespace.Globus;
import edu.isi.pegasus.planner.namespace.Pegasus;
import org.junit.Before;
import org.junit.Test;

/**
 * To test the Condor style class for condor code generator.
 *
 * @author vahi
 */
public class CondorGTest {
    private CondorG cs = null;

    public CondorGTest() {}

    @Before
    public void setUp() {
        cs = new CondorG();
    }

    @Test
    public void testPegasusProfileCores() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.CORES_KEY, "5");
        testForKey(j, Globus.COUNT_KEY, "5");
    }

    @Test
    public void testPegasusProfileCoresAndGlobusKey() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.CORES_KEY, "5");
        j.globusRSL.checkKeyInNS(Globus.COUNT_KEY, "6");
        testForKey(j, Globus.COUNT_KEY, "6");
    }

    @Test
    public void testPegasusProfileHostCount() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.NODES_KEY, "5");
        testForKey(j, Globus.HOST_COUNT_KEY, "5");
    }

    @Test
    public void testPegasusProfileHostCountAndGlobusKey() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.NODES_KEY, "5");
        j.globusRSL.checkKeyInNS(Globus.HOST_COUNT_KEY, "6");
        testForKey(j, Globus.HOST_COUNT_KEY, "6");
    }

    @Test
    public void testPegasusProfileRuntime() throws CondorStyleException {
        Job j = new Job();
        // runtime in seconds. walltime in minutes
        j.vdsNS.checkKeyInNS(Pegasus.RUNTIME_KEY, "5");
        testForKey(j, Globus.MAX_WALLTIME_KEY, "1");
    }

    @Test
    public void testPegasusProfileRuntimeAndGlobusKey() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.RUNTIME_KEY, "5");
        j.globusRSL.checkKeyInNS(Globus.MAX_WALLTIME_KEY, "6");
        testForKey(j, Globus.MAX_WALLTIME_KEY, "6");
    }

    @Test
    public void testPegasusProfileMemory() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.MEMORY_KEY, "5");
        testForKey(j, Globus.MAX_MEMORY_KEY, "5");
    }

    @Test
    public void testPegasusProfileMemoryAndGlobusKey() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.checkKeyInNS(Pegasus.MEMORY_KEY, "5");
        j.globusRSL.checkKeyInNS(Globus.MAX_MEMORY_KEY, "6");
        testForKey(j, Globus.MAX_MEMORY_KEY, "6");
    }

    private void testForKey(Job j, String key, String expectedValue) throws CondorStyleException {
        cs.handleResourceRequirements(j);
        assertTrue(j.globusRSL.containsKey(key));
        assertEquals(expectedValue, j.globusRSL.get(key));
    }
}
