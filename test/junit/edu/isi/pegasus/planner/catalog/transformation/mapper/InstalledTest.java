/**
 * Copyright 2007-2013 University Of Southern California
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.catalog.transformation.mapper;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.TCMap;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Installed TC mapper. */
public class InstalledTest {

    private static class TestInstalled extends Installed {
        TestInstalled(PegasusBag bag) {
            super(bag);
        }

        void setTCMapForTest(TCMap cache) {
            this.mTCMap = cache;
        }
    }

    private PegasusBag mBag;
    private TestSetup mTestSetup;
    private TestInstalled mMapper;

    @BeforeEach
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, properties);
        LogManager logger = mTestSetup.loadLogger(properties);
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);
        mMapper = new TestInstalled(mBag);
    }

    @Test
    public void testInstalledMapperCanBeInstantiated() {
        assertThat(mMapper, is(notNullValue()));
    }

    @Test
    public void testGetModeReturnsNonNullString() {
        assertThat(mMapper.getMode(), is(notNullValue()));
    }

    @Test
    public void testGetModeContainsInstalledKeyword() {
        String mode = mMapper.getMode();
        assertThat(mode.toLowerCase(), containsString("install"));
    }

    @Test
    public void testIsNotStageableMapper() {
        // Installed is not a Staged or Submit mapper
        assertThat(mMapper.isStageableMapper(), is(false));
    }

    @Test
    public void testIsInstanceOfMapper() {
        assertThat(
                mMapper,
                is(
                        org.hamcrest.Matchers.instanceOf(
                                edu.isi.pegasus.planner.catalog.transformation.Mapper.class)));
    }

    @Test
    public void testGetModeIsNonEmpty() {
        assertThat(mMapper.getMode().isEmpty(), is(false));
    }

    @Test
    public void testGetModeMatchesExpectedDescription() {
        assertThat(
                mMapper.getMode(),
                is("Installed Mode : Only use Installed executables at the site"));
    }

    @Test
    public void testGetSiteMapReturnsCachedEntriesWhenAllSitesArePresent() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "keg", "1.0");
        entry.setResourceId("isi");
        entry.setPhysicalTransformation("/bin/keg");
        entry.setType(TCType.INSTALLED);

        TCMap cache = new TCMap();
        cache.setSiteTCEntries(Separator.combine("ns", "keg", "1.0"), "isi", entry);
        mMapper.setTCMapForTest(cache);

        Map result = mMapper.getSiteMap("ns", "keg", "1.0", Arrays.asList("isi"));

        assertThat(result, is(notNullValue()));
        assertThat(result.size(), is(1));
        assertThat(
                result.get("isi"), is(sameInstance(cache.getSiteTCEntries("ns::keg:1.0", "isi"))));
    }

    @Test
    public void testGetSiteMapReturnsOnlyCachedSubsetForRequestedSites() {
        TransformationCatalogEntry isiEntry = new TransformationCatalogEntry("ns", "keg", "1.0");
        isiEntry.setResourceId("isi");
        isiEntry.setPhysicalTransformation("/bin/keg-isi");
        isiEntry.setType(TCType.INSTALLED);

        TransformationCatalogEntry localEntry = new TransformationCatalogEntry("ns", "keg", "1.0");
        localEntry.setResourceId("local");
        localEntry.setPhysicalTransformation("/bin/keg-local");
        localEntry.setType(TCType.INSTALLED);

        TCMap cache = new TCMap();
        cache.setSiteTCEntries("ns::keg:1.0", "isi", isiEntry);
        cache.setSiteTCEntries("ns::keg:1.0", "local", localEntry);
        mMapper.setTCMapForTest(cache);

        Map result = mMapper.getSiteMap("ns", "keg", "1.0", Arrays.asList("local"));

        assertThat(result, is(notNullValue()));
        assertThat(result.size(), is(1));
        assertThat(result.containsKey("local"), is(true));
        assertThat(result.containsKey("isi"), is(false));
    }

    @Test
    public void testInheritedGetTCListUsesCachedInstalledEntries() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "keg", "1.0");
        entry.setResourceId("isi");
        entry.setPhysicalTransformation("/bin/keg");
        entry.setType(TCType.INSTALLED);

        TCMap cache = new TCMap();
        cache.setSiteTCEntries("ns::keg:1.0", "isi", entry);
        mMapper.setTCMapForTest(cache);

        List result = mMapper.getTCList("ns", "keg", "1.0", "isi");

        assertThat(result, is(notNullValue()));
        assertThat(result.size(), is(1));
        assertThat(result.get(0), is(sameInstance(entry)));
    }

    @Test
    public void testInheritedGetSiteListUsesCachedInstalledEntries() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "keg", "1.0");
        entry.setResourceId("isi");
        entry.setPhysicalTransformation("/bin/keg");
        entry.setType(TCType.INSTALLED);

        TCMap cache = new TCMap();
        cache.setSiteTCEntries("ns::keg:1.0", "isi", entry);
        mMapper.setTCMapForTest(cache);

        List result = mMapper.getSiteList("ns", "keg", "1.0", Arrays.asList("isi"));

        assertThat(result.size(), is(1));
        assertThat(result.get(0), is("isi"));
    }
}
