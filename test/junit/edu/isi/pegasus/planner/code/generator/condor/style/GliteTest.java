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

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Before;
import org.junit.Test;

/**
 * To test the glite style class for condor code generator.
 *
 * @author vahi
 */
public class GliteTest {

    public static final String DEFAULT_GRID_RESOURCE = GLite.PBS_GRID_RESOURCE;

    private GLite gs = null;

    public GliteTest() {}

    @Before
    public void setUp() throws CondorStyleException {
        gs = new GLite();
        PegasusBag bag = new PegasusBag();
        LogManager logger = LogManagerFactory.loadSingletonInstance();
        logger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);
        logger.logEventStart("glite-test", "key", "value");
        gs.initialize(bag, null);
    }

    @Test
    public void testBasicPBSTimestampFormatting() {
        assertEquals("00:00:00", gs.pbsFormattedTimestamp("0"));
    }

    @Test
    public void testMinutePBSTimestampFormatting() {
        assertEquals("00:11:00", gs.pbsFormattedTimestamp("11"));
    }

    @Test
    public void testHourConversionPBSTimestampFormatting() {
        assertEquals("01:00:00", gs.pbsFormattedTimestamp("60"));
    }

    @Test
    public void test2HourConversionPBSTimestampFormatting() {
        assertEquals("01:09:00", gs.pbsFormattedTimestamp("69"));
    }

    @Test
    public void test3HourConversionPBSTimestampFormatting() {
        assertEquals("02:49:00", gs.pbsFormattedTimestamp("169"));
    }

    @Test
    public void test4HourConversionPBSTimestampFormatting() {
        assertEquals("28:10:00", gs.pbsFormattedTimestamp("1690"));
    }

    @Test
    public void testPegasusProfileHostCount() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.NODES_KEY, "5");
        this.testWithRegex(j, DEFAULT_GRID_RESOURCE, ".*NODES==\"([0-9]*)\".*", "5");
    }

    @Test
    public void testPegasusProfileMemory() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.MEMORY_KEY, "50");
        this.testWithRegex(j, DEFAULT_GRID_RESOURCE, ".*PER_PROCESS_MEMORY==\"([0-9]*)\".*", "50");
    }

    @Test
    public void testPegasusProfileMAXWalltime() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.RUNTIME_KEY, "100");
        this.testWithRegex(
                j,
                DEFAULT_GRID_RESOURCE,
                ".*WALLTIME==\"([0-9]+\\:[0-9]+\\:[0-9]+)\".*",
                "00:02:00");
    }

    @Test
    public void testPegasusProfilePPN() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.PPN_KEY, "100");
        this.testWithRegex(j, DEFAULT_GRID_RESOURCE, ".*PROCS==\"([0-9]*)\".*", "100");
    }

    @Test
    public void testGlobusProfileXCount() throws CondorStyleException {
        Job j = new Job();
        j.globusRSL.construct("xcount", "100");
        this.testWithRegex(j, DEFAULT_GRID_RESOURCE, ".*PROCS==\"([0-9]*)\".*", "100");
    }

    @Test(expected = CondorStyleException.class)
    public void testPegasusProfileCores() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.CORES_KEY, "5");
        gs.getCERequirementsForJob(j, DEFAULT_GRID_RESOURCE);
    }

    @Test
    public void testSGEPegasusProfileCores() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.CORES_KEY, "5");
        this.testWithRegex(j, "sge", ".*CORES==\"([0-9]*)\".*", "5");
    }

    // test SGE complex combinations
    @Test(expected = CondorStyleException.class)
    public void testSGEPegasusProfileNodes() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.NODES_KEY, "5");
        gs.getCERequirementsForJob(j, GLite.SGE_GRID_RESOURCE);
    }

    @Test(expected = CondorStyleException.class)
    public void testSGEPegasusProfilePPN() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.PPN_KEY, "5");
        gs.getCERequirementsForJob(j, GLite.SGE_GRID_RESOURCE);
    }

    @Test
    public void testSGEPegasusProfileNodesPPN() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.PPN_KEY, "8");
        j.vdsNS.construct(Pegasus.NODES_KEY, "5");
        this.testWithRegex(j, GLite.SGE_GRID_RESOURCE, ".*CORES==\"([0-9]*)\".*", "40");
    }

    // test PBS complex combinations
    public void testPBSPegasusProfileNodes() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.NODES_KEY, "5");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*NODES==\"([0-9]*)\".*", "5");
    }

    public void testPBSPegasusProfilePPN() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.PPN_KEY, "8");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*PROCS==\"([0-9]*)\".*", "8");
    }

    @Test
    public void testPBSPegasusProfileNodesPPN() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.PPN_KEY, "8");
        j.vdsNS.construct(Pegasus.NODES_KEY, "5");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*NODES==\"([0-9]*)\".*", "5");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*PROCS==\"([0-9]*)\".*", "8");
    }

    @Test
    public void testPBSPegasusCoresNodes() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.CORES_KEY, "40");
        j.vdsNS.construct(Pegasus.NODES_KEY, "5");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*NODES==\"([0-9]*)\".*", "5");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*PROCS==\"([0-9]*)\".*", "8");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*CORES==\"([0-9]*)\".*", "40");
    }

    @Test(expected = CondorStyleException.class)
    public void testPBSPegasusCoresNodesInvalid() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.CORES_KEY, "42");
        j.vdsNS.construct(Pegasus.NODES_KEY, "5");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*NODES==\"([0-9]*)\".*", "5");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*PROCS==\"([0-9]*)\".*", "8");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*CORES==\"([0-9]*)\".*", "40");
    }

    @Test
    public void testPBSPegasusCoresPPN() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.CORES_KEY, "40");
        j.vdsNS.construct(Pegasus.PPN_KEY, "8");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*NODES==\"([0-9]*)\".*", "5");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*PROCS==\"([0-9]*)\".*", "8");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*CORES==\"([0-9]*)\".*", "40");
    }

    @Test(expected = CondorStyleException.class)
    public void testPBSPegasusCoresPPNCeiling() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.CORES_KEY, "42");
        j.vdsNS.construct(Pegasus.PPN_KEY, "8");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*NODES==\"([0-9]*)\".*", "6");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*PROCS==\"([0-9]*)\".*", "8");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*CORES==\"([0-9]*)\".*", "42");
    }

    @Test
    public void testPBSPegasusCoresNodesPPN() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.CORES_KEY, "40");
        j.vdsNS.construct(Pegasus.NODES_KEY, "5");
        j.vdsNS.construct(Pegasus.PPN_KEY, "8");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*NODES==\"([0-9]*)\".*", "5");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*PROCS==\"([0-9]*)\".*", "8");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*CORES==\"([0-9]*)\".*", "40");
    }

    @Test(expected = CondorStyleException.class)
    public void testPBSPegasusCoresNodesPPNInvalid() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.CORES_KEY, "42");
        j.vdsNS.construct(Pegasus.NODES_KEY, "5");
        j.vdsNS.construct(Pegasus.PPN_KEY, "8");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*NODES==\"([0-9]*)\".*", "5");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*PROCS==\"([0-9]*)\".*", "8");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*CORES==\"([0-9]*)\".*", "40");
    }

    @Test
    public void testPBSPegasusCoresNodesPPNAllOnes() throws CondorStyleException {
        Job j = new Job();
        j.vdsNS.construct(Pegasus.CORES_KEY, "1");
        j.vdsNS.construct(Pegasus.NODES_KEY, "1");
        j.vdsNS.construct(Pegasus.PPN_KEY, "1");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*NODES==\"([0-9]*)\".*", "1");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*PROCS==\"([0-9]*)\".*", "1");
        this.testWithRegex(j, GLite.PBS_GRID_RESOURCE, ".*CORES==\"([0-9]*)\".*", "1");
    }

    private void testWithRegex(Job j, String gridResource, String regex, String expected)
            throws CondorStyleException {
        String ce = gs.getCERequirementsForJob(j, gridResource);
        // System.out.println( ce );
        Pattern p = Pattern.compile(regex);
        Matcher m = p.matcher(ce);
        String value = null;
        while (m.find()) {
            value = m.group(1);
        }
        assertEquals(expected, value);
    }
}
