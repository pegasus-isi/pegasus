/**
 *  Copyright 2007-2013 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package edu.isi.pegasus.planner.transfer.mapper;

import edu.isi.pegasus.planner.catalog.site.classes.FileServerType;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * A JUnit Test to test the output mapper interface.
 *
 * @author Karan Vahi
 */
public class FlatOutputMapperTest extends OutputMapperTest {
   

    /**
     * Test of the Flat Output Mapper.
     */
    @Test
    public void test() {
        
        int set = 1;
        mLogger.logEventStart( "test.output.mapper.Flat", "set", Integer.toString(set++) );
        mProps.setProperty( OutputMapperFactory.PROPERTY_KEY, "Flat" );
        OutputMapper mapper = OutputMapperFactory.loadInstance( new ADag(), mBag );
        
        String lfn    = "f.a";
        String pfn    = mapper.map( lfn, "local", FileServerType.OPERATION.put );
        assertEquals( lfn + " not mapped to right location " ,
                      "file:///test/junit/output/mapper/blackdiamond/outputs/f.a", pfn );
        
        pfn = mapper.map( lfn, "local", FileServerType.OPERATION.get );
        assertEquals( lfn + " not mapped to right location " ,
                      "gsiftp://sukhna.isi.edu/test/junit/output/mapper/blackdiamond/outputs/f.a", pfn );
        
        List<String> pfns = mapper.mapAll( lfn, "local", FileServerType.OPERATION.get);
        String[] expected = { "gsiftp://sukhna.isi.edu/test/junit/output/mapper/blackdiamond/outputs/f.a" };
        assertArrayEquals( expected, pfns.toArray() );
        mLogger.logEventCompletion();
        
        
        //set property to enable deep storage, where relative submit directory is added
        mLogger.logEventStart( "test.output.mapper.Flat", "set", Integer.toString(set++) );
        mBag.getPlannerOptions().setRelativeDirectory( "deep" );
        mProps.setProperty( "pegasus.dir.storage.deep", "true" );
        SiteStore store = mBag.getHandleToSiteStore();
        store.setForPlannerUse(mProps,mBag.getPlannerOptions() );
        mapper = OutputMapperFactory.loadInstance( new ADag(), mBag );
        
        String deepPFN    = mapper.map( lfn, "local", FileServerType.OPERATION.put );
        assertEquals( lfn + " not mapped to right location " ,
                      "file:///test/junit/output/mapper/blackdiamond/outputs/deep/f.a", deepPFN );
        
        deepPFN = mapper.map( lfn, "local", FileServerType.OPERATION.get );
        assertEquals( lfn + " not mapped to right location " ,
                      "gsiftp://sukhna.isi.edu/test/junit/output/mapper/blackdiamond/outputs/deep/f.a", deepPFN );
        
        List<String> deepPFNS = mapper.mapAll( lfn, "local", FileServerType.OPERATION.get);
        String[] expectedDeepPFNS = { "gsiftp://sukhna.isi.edu/test/junit/output/mapper/blackdiamond/outputs/deep/f.a" };
        assertArrayEquals( expectedDeepPFNS, deepPFNS.toArray() );
        mLogger.logEventCompletion();
    }

    

    @Override
    protected String getPropertiesFileBasename() {
        return "flat.properties";
    }

    
    
    
}