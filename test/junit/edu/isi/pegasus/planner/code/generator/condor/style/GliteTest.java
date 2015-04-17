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

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
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
}
