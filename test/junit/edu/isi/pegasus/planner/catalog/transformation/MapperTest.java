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
package edu.isi.pegasus.planner.catalog.transformation;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.transformation.mapper.Staged;
import edu.isi.pegasus.planner.catalog.transformation.mapper.Submit;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.TCMap;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.test.DefaultTestSetup;
import edu.isi.pegasus.planner.test.TestSetup;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Tests for the abstract Mapper class — exercised via an anonymous concrete subclass and the static
 * constants.
 */
public class MapperTest {

    private TestSetup mTestSetup;
    private PegasusBag mBag;

    @BeforeEach
    public void setUp() {
        mTestSetup = new DefaultTestSetup();
        mBag = new PegasusBag();
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        mBag.add(PegasusBag.PEGASUS_PROPERTIES, properties);
        LogManager logger = mTestSetup.loadLogger(properties);
        mBag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);
    }

    @Test
    public void testPackageNameConstantIsCorrect() {
        assertThat(
                Mapper.PACKAGE_NAME, is("edu.isi.pegasus.planner.catalog.transformation.mapper"));
    }

    @Test
    public void testPackageNameConstantIsNonEmpty() {
        assertThat(Mapper.PACKAGE_NAME, is(notNullValue()));
        assertThat(Mapper.PACKAGE_NAME.isEmpty(), is(false));
    }

    @Test
    public void testAnonymousSubclassCanBeInstantiated() {
        Mapper mapper = createMinimalMapper();
        assertThat(mapper, is(notNullValue()));
    }

    @Test
    public void testGetModeReturnedBySubclass() {
        Mapper mapper = createMinimalMapper();
        assertThat(mapper.getMode(), is("TestMode"));
    }

    @Test
    public void testIsStageableMapperReturnsFalseForNonStageableMapper() {
        Mapper mapper = createMinimalMapper();
        // The anonymous subclass is neither Staged nor Submit, so should be false
        assertThat(mapper.isStageableMapper(), is(false));
    }

    @Test
    public void testIsStageableMapperReturnsTrueForStagedMapper() {
        assertThat(new Staged(mBag).isStageableMapper(), is(true));
    }

    @Test
    public void testIsStageableMapperReturnsTrueForSubmitMapper() {
        assertThat(new Submit(mBag).isStageableMapper(), is(true));
    }

    @Test
    public void testLoadTCMapperLoadsStagedMapperByShortName() {
        Mapper mapper = Mapper.loadTCMapper("Staged", mBag);

        assertThat(mapper, instanceOf(Staged.class));
        assertThat(mapper.isStageableMapper(), is(true));
    }

    @Test
    public void testGetTCListReturnsEntriesForRequestedSite() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "keg", "1.0");
        TestingMapper mapper =
                createPopulatedMapper(
                        entry, Collections.singletonMap("siteA", Collections.singletonList(entry)));

        List entries = mapper.getTCList("ns", "keg", "1.0", "siteA");

        assertThat(entries.size(), is(1));
        assertThat(entries.get(0), is(sameInstance(entry)));
    }

    @Test
    public void testGetTCListReturnsNullWhenSiteMapLookupReturnsNull() {
        Mapper mapper = createMinimalMapper();

        assertThat(mapper.getTCList("ns", "keg", "1.0", "siteA"), is(nullValue()));
    }

    @Test
    public void testGetSiteListReturnsIntersectionOfRequestedSitesAndTCMapSites() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "keg", "1.0");
        Map<String, List> siteMap = new HashMap<>();
        siteMap.put("siteA", Collections.singletonList(entry));
        siteMap.put("siteB", Collections.singletonList(entry));
        TestingMapper mapper = createPopulatedMapper(entry, siteMap);

        List result =
                mapper.getSiteList(
                        "ns", "keg", "1.0", new ArrayList<>(Arrays.asList("siteB", "siteC")));

        assertThat(result.size(), is(1));
        assertThat(result.get(0), is("siteB"));
    }

    @Test
    public void testIsSiteValidReturnsTrueWhenMapContainsSite() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry("ns", "keg", "1.0");
        TestingMapper mapper =
                createPopulatedMapper(
                        entry, Collections.singletonMap("siteA", Collections.singletonList(entry)));

        assertThat(mapper.isSiteValid("ns", "keg", "1.0", "siteA"), is(true));
    }

    @Test
    public void testIsSiteValidReturnsFalseWhenMapIsEmpty() {
        TestingMapper mapper = new TestingMapper(mBag, Collections.emptyMap());

        assertThat(mapper.isSiteValid("ns", "keg", "1.0", "siteA"), is(false));
    }

    // Helper that constructs a trivial concrete Mapper for testing abstract class methods.
    private Mapper createMinimalMapper() {
        return new Mapper(mBag) {
            @Override
            public Map getSiteMap(String namespace, String name, String version, List siteids) {
                return null;
            }

            @Override
            public String getMode() {
                return "TestMode";
            }
        };
    }

    private TestingMapper createPopulatedMapper(
            TransformationCatalogEntry entry, Map<String, List> siteMap) {
        return new TestingMapper(mBag, siteMap);
    }

    private static class TestingMapper extends Mapper {
        private final Map<String, List> mSiteMapToReturn;

        private TestingMapper(PegasusBag bag, Map<String, List> siteMapToReturn) {
            super(bag);
            mSiteMapToReturn = siteMapToReturn;
        }

        @Override
        public Map getSiteMap(String namespace, String name, String version, List siteids) {
            String lfn = edu.isi.pegasus.common.util.Separator.combine(namespace, name, version);
            mTCMap = new TCMap();
            for (Map.Entry<String, List> entry : mSiteMapToReturn.entrySet()) {
                for (Object value : entry.getValue()) {
                    mTCMap.setSiteTCEntries(
                            lfn, entry.getKey(), (TransformationCatalogEntry) value);
                }
            }
            return mSiteMapToReturn;
        }

        @Override
        public String getMode() {
            return "Testing";
        }
    }
}
