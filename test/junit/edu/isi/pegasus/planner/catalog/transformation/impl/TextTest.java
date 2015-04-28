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

package edu.isi.pegasus.planner.catalog.transformation.impl;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.LinkedList;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

import java.io.File;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Test;
/**
 *
 * A Test class to test the Textual format of the Transformation Catalog
 * 
 * @author Karan Vahi
 */
public class TextTest {
    /**
     * The properties used for this test.
     */
    private static final String PROPERTIES_BASENAME="properties";
    
    private PegasusBag mBag;
    
    private PegasusProperties mProps;
    
    private LogManager mLogger;
    
    private TestSetup mTestSetup;
    
    private static int mTestNumber =1 ;
    private Text mCatalog;
    
    
    @BeforeClass
    public static void setUpClass() {
        Map<String,String> testEnvVariables = new HashMap();
        testEnvVariables.put( "site", "test-site" );
       
        setEnv( testEnvVariables );
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    public TextTest(){
        
    }
    
    /**
     * Hackish way to setup environment for this test case.
     * 
     * http://stackoverflow.com/questions/318239/how-do-i-set-environment-variables-from-java
     * 
     */
    public static void setEnv(Map<String, String> newEnv ) {
        try {
            Class<?> processEnvironmentClass = Class.forName("java.lang.ProcessEnvironment");
            Field theEnvironmentField = processEnvironmentClass.getDeclaredField("theEnvironment");
            theEnvironmentField.setAccessible(true);
            Map<String, String> env = (Map<String, String>) theEnvironmentField.get(null);
            env.putAll( newEnv );
            Field theCaseInsensitiveEnvironmentField = processEnvironmentClass.getDeclaredField("theCaseInsensitiveEnvironment");
            theCaseInsensitiveEnvironmentField.setAccessible(true);
            Map<String, String> cienv = (Map<String, String>) theCaseInsensitiveEnvironmentField.get(null);
            cienv.putAll( newEnv );
        } catch (NoSuchFieldException e) {
            try {
                Class[] classes = Collections.class.getDeclaredClasses();
                Map<String, String> env = System.getenv();
                for (Class cl : classes) {
                    if ("java.util.Collections$UnmodifiableMap".equals(cl.getName())) {
                        Field field = cl.getDeclaredField("m");
                        field.setAccessible(true);
                        Object obj = field.get(env);
                        Map<String, String> map = (Map<String, String>) obj;
                        map.clear();
                        map.putAll( newEnv );
                    }
                }
            } catch (Exception e2) {
                e2.printStackTrace();
            }
        } catch (Exception e1) {
            e1.printStackTrace();
        }
    }
    
    /**
     * Setup the logger and properties that all test functions require
     */
    @Before
    public final void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        mTestSetup.setInputDirectory( this.getClass() );
        System.out.println( "Input Test Dir is " + mTestSetup.getInputDirectory() );
        
        mProps = mTestSetup.loadPropertiesFromFile( PROPERTIES_BASENAME, new LinkedList());
        
        mLogger  = mTestSetup.loadLogger( mProps );
        mLogger.setLevel( LogManager.INFO_MESSAGE_LEVEL );
        mLogger.logEventStart( "test.catalog.transformation.impl.Text", "setup", "0" );
        mBag.add( PegasusBag.PEGASUS_LOGMANAGER, mLogger );
        mBag.add( PegasusBag.PEGASUS_PROPERTIES, mProps );
        //mBag.add( PegasusBag.PLANNER_OPTIONS, mTestSetup.loadPlannerOptions() );
        
        //load the transformation catalog backend
        mCatalog = new Text();
        mProps.setProperty( PegasusProperties.PEGASUS_TRANSFORMATION_CATALOG_FILE_PROPERTY, 
                            new File( mTestSetup.getInputDirectory(), "tc.text" ).getAbsolutePath() );
        mCatalog.initialize( mBag );
        mLogger.logEventCompletion();
    }

    @Test
    public void testWholeCount() throws Exception {
        mLogger.logEventStart( "test.catalog.transformation.impl.Text", "whole-count-test", Integer.toString(mTestNumber++) );
        List<TransformationCatalogEntry> entries = mCatalog.getContents();
        assertEquals( "Expected total number of entries", 3 , entries.size() );
        List<TransformationCatalogEntry> kegEntries = mCatalog.lookup( "example", "keg", "1.0", (String)null, null );
        assertEquals( "Expected total number of keg entries", 2 , kegEntries.size() );
        mLogger.logEventCompletion();
    }
    
    @Test
    public void testKegCount() throws Exception {
        mLogger.logEventStart( "test.catalog.transformation.impl.Text", "keg-count-test", Integer.toString(mTestNumber++) );
        List<TransformationCatalogEntry> kegEntries = mCatalog.lookup( "example", "keg", "1.0", (String)null, null );
        assertEquals( "Expected total number of keg entries", 2 , kegEntries.size() );
        mLogger.logEventCompletion();
        
    }
    
}
