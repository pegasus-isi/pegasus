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

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.code.generator.condor.CondorStyleException;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * To test the glite style class for condor code generator.
 * 
 * @author vahi
 */
public class GliteTest {
    private GLite gs = null;

    public GliteTest() {
    }

    @Before
    public void setUp() {
        gs = new GLite();
    }
    
    
    @Test
    public void testBasicPBSTimestampFormatting() {
        assertEquals( "00:00:00", gs.pbsFormattedTimestamp( "0"));
    }
    
    
    @Test
    public void testMinutePBSTimestampFormatting() {
        assertEquals( "00:11:00", gs.pbsFormattedTimestamp( "11"));
    }
    
    @Test
    public void testHourConversionPBSTimestampFormatting() {
        assertEquals( "01:00:00", gs.pbsFormattedTimestamp( "60"));
    }
    
    @Test
    public void test2HourConversionPBSTimestampFormatting() {
        assertEquals( "01:09:00", gs.pbsFormattedTimestamp( "69"));
    }
    
    @Test
    public void test3HourConversionPBSTimestampFormatting() {
        assertEquals( "02:49:00", gs.pbsFormattedTimestamp( "169"));
    }
    
    @Test
    public void test4HourConversionPBSTimestampFormatting() {
        assertEquals( "28:10:00", gs.pbsFormattedTimestamp( "1690"));
    }
    
    /*@Test
    public void testPegasusProfileCores() throws CondorStyleException{
        Job j = new Job();
        j.vdsNS.construct( Pegasus.CORES_KEY, "5" );
        String ce = gs.getCERequirementsForJob( j );
        System.out.println( ce );
    }*/
    
    @Test
    public void testPegasusProfileHostCount() throws CondorStyleException{
        Job j = new Job();
        j.vdsNS.construct(Pegasus.NODES_KEY, "5" );
        String ce = gs.getCERequirementsForJob( j );
        this.testWithRegex(j, ".*NODES==\"([0-9]*)\".*", "5");
    }
    
    @Test
    public void testPegasusProfileMemory() throws CondorStyleException{
        Job j = new Job();
        j.vdsNS.construct( Pegasus.MEMORY_KEY, "50" );
        this.testWithRegex(j, ".*PER_PROCESS_MEMORY==\"([0-9]*)\".*", "50");
    }
    
    @Test
    public void testPegasusProfileMAXWalltime() throws CondorStyleException{
        Job j = new Job();
        j.vdsNS.construct( Pegasus.RUNTIME_KEY, "100" );
        this.testWithRegex(j, ".*WALLTIME==\"([0-9]+\\:[0-9]+\\:[0-9]+)\".*", "00:02:00");
    }
    
    @Test
    public void testGlobusProfileXCount() throws CondorStyleException{
        Job j = new Job();
        j.globusRSL.construct( "xcount", "100" );
        this.testWithRegex(j, ".*PROCS==\"([0-9]*)\".*", "100") ;
    }
    
    
    private void testWithRegex( Job j, String regex, String expected) throws CondorStyleException{
        String ce = gs.getCERequirementsForJob( j );
        //System.out.println( ce );
        Pattern p = Pattern.compile( regex );
        Matcher m = p.matcher( ce );
        String value = null;
        while(m.find()){
            value = m.group(1);
        }
        assertEquals( expected, value );
    }
}
