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

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.classes.ADag;
import java.util.List;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * A JUnit Test to test the Replica Output Mapper interface.
 *
 * @author Karan Vahi
 */
public class ReplicaOutputMapperTest extends OutputMapperTest {
   

    /**
     * Test of the Flat Output Mapper.
     */
    @Test
    public void test() {
        
        int set = 1;
        //test with no deep storage structure enabled
        mLogger.logEventStart( "test.output.mapper.Replica", "set", Integer.toString(set++) );
        mProps.setProperty( OutputMapperFactory.PROPERTY_KEY, "Replica" );
        OutputMapper mapper = OutputMapperFactory.loadInstance( new ADag(), mBag );
        
        for( FileServer.OPERATION operation : FileServer.OPERATION.values() ){
            //replica mapper maps all operations to the same pfn
            String lfn    = "f.a1";
            String expected= "gsiftp://corbusier.isi.edu/Volumes/data/output/nonregex/" + lfn;
            String pfn    = mapper.map( lfn, "local", operation);
            assertEquals( lfn + " not mapped to right location " , expected, pfn );
            String[] expectedPFNS = {pfn};
            List<String>pfns    = mapper.mapAll( lfn, "local", operation);
            assertArrayEquals( expectedPFNS, pfns.toArray() );
        }
        mLogger.logEventCompletion();
        
        //test to make sure that PFN constructed from regex works fine
        mLogger.logEventStart( "test.output.mapper.Replica", "set", Integer.toString(set++) );
        for( int i = 2; i <= 10; i++ ){
            String lfn = "f.a" + i; 
            for( FileServer.OPERATION operation : FileServer.OPERATION.values() ){
                //replica mapper maps all operations to the same pfn
                String expected= "gsiftp://corbusier.isi.edu/Volumes/data/output/" + lfn;
                String pfn    = mapper.map( lfn, "local", operation);
                assertEquals( lfn + " not mapped to right location " , expected, pfn );
                String[] expectedPFNS = {pfn};
                List<String>pfns    = mapper.mapAll( lfn, "local", operation);
                assertArrayEquals( expectedPFNS, pfns.toArray() );
            }
        }
        mLogger.logEventCompletion();
        
        /*
        pfn = mapper.map( lfn, "local", FileServerType.OPERATION.get );
        assertEquals( lfn + " not mapped to right location " , expected, pfn );
        
        List<String> pfns = mapper.mapAll( lfn, "local", FileServerType.OPERATION.get);
        String[] expectedPFNS = { "gsiftp://sukhna.isi.edu/test/junit/output/mapper/blackdiamond/outputs/f.a" };
        assertArrayEquals( expectedPFNS, pfns.toArray() );
        mLogger.logEventCompletion();
        
        ////test with  deep storage structure enabled
        //set property to enable deep storage, where relative submit directory is added
        mLogger.logEventStart( "test.output.mapper.Replica", "set", Integer.toString(set++) );
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
        */
    }

    /**
     * Returns the list of property keys that should be sanitized
     * 
     * @return List<String>
     */
    protected List<String> getPropertyKeysForSanitization(){
        List<String> keys = super.getPropertyKeysForSanitization();
        keys.add( "pegasus.dir.storage.mapper.replica.file" );
        return keys;
    }


    @Override
    protected String getPropertiesFileBasename() {
        return "replica.properties";
    }

    
    
    
}