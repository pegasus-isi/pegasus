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
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.classes.SysInfo.*;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;

import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.EnvSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.LinkedList;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import static org.junit.Assert.*;

import java.io.File;
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
    
    private static final String EXPANDED_SITE        = "bamboo";
    private static final String EXPANDED_NAMESPACE   = "pegasus";
    private static final String EXPANDED_NAME        = "keg";
    private static final String EXPANDED_VERSION     = "version";
    private static final String EXPANDED_ARCH        = "x86_64";
    private static final String EXPANDED_OS          = "LINUX";
    private static final String EXPANDED_KEG_PATH    = "file:///usr/bin/pegasus-keg";
    
    private PegasusBag mBag;
    
    private PegasusProperties mProps;
    
    private LogManager mLogger;
    
    private TestSetup mTestSetup;
    
    private static int mTestNumber =1 ;
    private Text mCatalog;
    
    
    @BeforeClass
    public static void setUpClass() {
        Map<String,String> testEnvVariables = new HashMap();
        testEnvVariables.put( "SITE", EXPANDED_SITE );
        testEnvVariables.put( "NAMESPACE", EXPANDED_NAMESPACE );
        testEnvVariables.put( "NAME", EXPANDED_NAME );
        testEnvVariables.put( "VERSION", EXPANDED_VERSION );
        testEnvVariables.put( "ARCH", EXPANDED_ARCH );
        testEnvVariables.put( "OS",  EXPANDED_OS );
        testEnvVariables.put(  "KEGPATH", EXPANDED_KEG_PATH );
        EnvSetup.setEnvironmentVariables(testEnvVariables);
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    public TextTest(){
        
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
        assertEquals( "Expected total number of entries", 4, entries.size() );
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
    
    @Test
    public void testParameterExpansionCount() throws Exception {
        mLogger.logEventStart( "test.catalog.transformation.impl.Text", "parameter-expansion-test", Integer.toString(mTestNumber++) );
        List<TransformationCatalogEntry> kegEntries = mCatalog.lookup( TextTest.EXPANDED_NAMESPACE, TextTest.EXPANDED_NAME, TextTest.EXPANDED_VERSION, TextTest.EXPANDED_SITE, null );
        assertEquals( "Expected total number of keg entries", 1 , kegEntries.size() );
        mLogger.logEventCompletion();
        
    }
    
    @Test
    public void testParameterExpansionContents() throws Exception {
        mLogger.logEventStart( "test.catalog.transformation.impl.Text", "parameter-expansion-contents", Integer.toString(mTestNumber++) );
        List<TransformationCatalogEntry> kegEntries = mCatalog.lookup( TextTest.EXPANDED_NAMESPACE, TextTest.EXPANDED_NAME, TextTest.EXPANDED_VERSION, TextTest.EXPANDED_SITE, null );
        TransformationCatalogEntry expanded = kegEntries.get( 0 );
        SysInfo info = expanded.getSysInfo();
        assertEquals( "Expected attribute ", EXPANDED_NAMESPACE , expanded.getLogicalNamespace() );
        assertEquals( "Expected attribute ", EXPANDED_NAME ,      expanded.getLogicalName() );
        assertEquals( "Expected attribute ", EXPANDED_VERSION ,   expanded.getLogicalVersion() );
        assertEquals( "Expected attribute ", EXPANDED_SITE ,      expanded.getResourceId() );
        assertEquals( "Expected attribute ", EXPANDED_KEG_PATH ,  expanded.getPhysicalTransformation() );
        assertEquals( "Expected attribute ", EXPANDED_ARCH ,      info.getArchitecture().name() );
        assertEquals( "Expected attribute ", EXPANDED_OS ,        info.getOS().name() );
        
        mLogger.logEventCompletion();
        
    }
    
    @Test
    public void testMetadataKeyword() throws Exception {
        mLogger.logEventStart( "test.catalog.transformation.impl.Text", "metadata-keyword", Integer.toString(mTestNumber++) );
        List<TransformationCatalogEntry> entries = mCatalog.lookup( null, "myxform", null, "condorpool", null );
        TransformationCatalogEntry entry = entries.get( 0 );
        SysInfo info = entry.getSysInfo();
        assertEquals("Expected attribute ", null , entry.getLogicalNamespace() );
        assertEquals("Expected attribute ", "myxform" ,      entry.getLogicalName() );
        assertEquals("Expected attribute ", null ,   entry.getLogicalVersion() );
        assertEquals("Expected attribute ", "condorpool" ,      entry.getResourceId() );
        assertEquals("Expected attribute ", "/usr/bin/true" ,  entry.getPhysicalTransformation() );
        assertEquals( "Expected attribute ", Architecture.x86_64.toString(), info.getArchitecture().name() );
        assertEquals( "Expected attribute ", OS.linux.toString(), info.getOS().name() );
        testProfile( entry, Profile.METADATA, "key", "value" );
        testProfile( entry, Profile.METADATA, "appmodel", "myxform.aspen" );
        testProfile( entry, Profile.METADATA, "version", "3.0" );
        
        mLogger.logEventCompletion();
        
    }

    private void testProfile(TransformationCatalogEntry entry, String namespace, String key, String value) {
        Profile p = new Profile( namespace, key, value );
        List profiles = entry.getProfiles( namespace );
        assertTrue( "Entry " + entry  , profiles.contains(p));
        
    }
    
}
