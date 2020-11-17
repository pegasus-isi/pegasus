/*
 *
 *   Copyright 2007-2020 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */
package edu.isi.pegasus.planner.catalog.transformation.mapper;

import static org.junit.Assert.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.parser.dax.DAXParser3Test;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author vahi */
public class AllTest {
    private PegasusBag mBag;

    private LogManager mLogger;

    private TestSetup mTestSetup;
    private All mMapper;

    public AllTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    @Before
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        // the input dir is from the dax subpackage
        mTestSetup.setInputDirectory(DAXParser3Test.class);

        System.out.println("Input Test Dir is " + mTestSetup.getInputDirectory());
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        mLogger = mTestSetup.loadLogger(properties);
        mLogger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
        mLogger.logEventStart("test.planner.catalog.transformation.mapper", "setup", "0");
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, properties);
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, mLogger);
        mLogger.logEventCompletion();
        mMapper = new All(mBag);
    }

    // public boolean match(TransformationCatalogEntry entry, String site, SysInfo sitesysinfo) {

    @Test
    public void matchDifferentSysInfo() {
        TransformationCatalogEntry t = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        SysInfo s = new SysInfo();
        s.setArchitecture(SysInfo.Architecture.x86_64);
        s.setOS(SysInfo.OS.macosx);
        t.setSysInfo(new SysInfo());
        assertFalse(this.mMapper.match(t, "isi", s));
    }

    @Test
    public void matchSameSysInfoWithInstalledType() {
        TransformationCatalogEntry t = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        t.setType(TCType.INSTALLED);
        t.setResourceId("isi");
        SysInfo s = new SysInfo();
        s.setArchitecture(SysInfo.Architecture.x86_64);
        s.setOS(SysInfo.OS.macosx);
        t.setSysInfo(s);
        assertTrue(this.mMapper.match(t, "isi", s));
    }

    @Test
    public void matchSameSysInfoWithStagedType() {
        TransformationCatalogEntry t = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        t.setType(TCType.STAGEABLE);
        SysInfo s = new SysInfo();
        s.setArchitecture(SysInfo.Architecture.x86_64);
        s.setOS(SysInfo.OS.macosx);
        t.setSysInfo(s);
        assertTrue(this.mMapper.match(t, "isi", s));
    }

    @Test
    public void matchInstalledTCWithEmptyContainer() {
        TransformationCatalogEntry t = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        t.setType(TCType.INSTALLED);
        SysInfo s = new SysInfo();
        s.setArchitecture(SysInfo.Architecture.x86_64);
        s.setOS(SysInfo.OS.macosx);
        t.setSysInfo(s);
        Container c = new Container("montage");
        assertFalse(this.mMapper.match(t, "isi", s));
    }

    @Test
    public void matchInstalledTCWithFileContainer() {
        TransformationCatalogEntry t = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        t.setType(TCType.INSTALLED);
        SysInfo s = new SysInfo();
        s.setArchitecture(SysInfo.Architecture.x86_64);
        s.setOS(SysInfo.OS.macosx);
        t.setSysInfo(s);
        Container c = new Container("montage");
        c.setImageURL("/scratch/montage.simg");
        t.setContainer(c);
        assertFalse(this.mMapper.match(t, "isi", s));
    }

    @Test
    public void matchInstalledTCWithHubContainer() {
        TransformationCatalogEntry t = new TransformationCatalogEntry("pegasus", "keg", "1.0");
        t.setType(TCType.INSTALLED);
        SysInfo s = new SysInfo();
        s.setArchitecture(SysInfo.Architecture.x86_64);
        s.setOS(SysInfo.OS.macosx);
        t.setSysInfo(s);
        Container c = new Container("montage");
        c.setImageURL("library://rynge/default/montage:latest");
        t.setContainer(c);
        assertTrue(this.mMapper.match(t, "isi", s));
    }
}
