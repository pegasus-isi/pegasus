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
package edu.isi.pegasus.planner.code;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.common.PegasusConfiguration;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.namespace.Pegasus;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for GridStartFactory constants and structure */
public class GridStartFactoryTest {

    private static final class TestGridStartFactory extends GridStartFactory {
        String shortNameFor(Job job) {
            return getGridStartShortName(job);
        }
    }

    @Test
    public void testDefaultPackageName() {
        assertThat(
                GridStartFactory.DEFAULT_PACKAGE_NAME,
                is("edu.isi.pegasus.planner.code.gridstart"));
    }

    @Test
    public void testDefaultGridstartMode() {
        assertThat(GridStartFactory.DEFAULT_GRIDSTART_MODE, is("Kickstart"));
    }

    @Test
    public void testKickstartIndex() {
        assertThat(GridStartFactory.KICKSTART_INDEX, is(0));
    }

    @Test
    public void testNoGridstartIndex() {
        assertThat(GridStartFactory.NO_GRIDSTART_INDEX, is(1));
    }

    @Test
    public void testGridstartShortNamesLength() {
        assertThat(GridStartFactory.GRIDSTART_SHORT_NAMES.length, is(2));
        assertThat(GridStartFactory.GRIDSTART_SHORT_NAMES[0], is("kickstart"));
        assertThat(GridStartFactory.GRIDSTART_SHORT_NAMES[1], is("none"));
    }

    @Test
    public void testGridstartImplementingClassesLength() {
        assertThat(GridStartFactory.GRIDSTART_IMPLEMENTING_CLASSES.length, is(2));
        assertThat(GridStartFactory.GRIDSTART_IMPLEMENTING_CLASSES[0], is("Kickstart"));
        assertThat(GridStartFactory.GRIDSTART_IMPLEMENTING_CLASSES[1], is("NoGridStart"));
    }

    @Test
    public void testPostScriptScopeConstants() {
        assertThat(GridStartFactory.ESSENTIAL_POST_SCRIPT_SCOPE, is("essential"));
        assertThat(GridStartFactory.ALL_POST_SCRIPT_SCOPE, is("all"));
    }

    @Test
    public void testLoadGridStartThrowsWhenFactoryNotInitialized() {
        GridStartFactory factory = new GridStartFactory();

        assertThrows(GridStartFactoryException.class, () -> factory.loadGridStart(new Job(), null));
    }

    @Test
    public void testLoadPOSTScriptThrowsWhenFactoryNotInitialized() {
        GridStartFactory factory = new GridStartFactory();

        assertThrows(
                GridStartFactoryException.class, () -> factory.loadPOSTScript(new Job(), null));
    }

    @Test
    public void testLoadPOSTScriptThrowsWhenGridStartIsNull() throws Exception {
        GridStartFactory factory = new GridStartFactory();
        ReflectionTestUtils.setField(factory, "mInitialized", Boolean.TRUE);

        GridStartFactoryException exception =
                assertThrows(
                        GridStartFactoryException.class,
                        () -> factory.loadPOSTScript(new Job(), null));

        assertThat(
                exception.getMessage().contains("supplied a GridStart implementation"), is(true));
    }

    @Test
    public void testGetGridStartShortNameUsesPegasusLitePrefixShortcut() throws Exception {
        TestGridStartFactory factory = configuredFactory(null);
        Job job = new Job();
        job.vdsNS.construct(Pegasus.GRIDSTART_KEY, "PegasusLite.Kickstart");

        assertThat(factory.shortNameFor(job), is("PegasusLite"));
    }

    @Test
    public void testGetGridStartShortNameDefaultsToKickstart() throws Exception {
        TestGridStartFactory factory = configuredFactory(null);

        assertThat(factory.shortNameFor(new Job()), is(GridStartFactory.DEFAULT_GRIDSTART_MODE));
    }

    @Test
    public void testGetGridStartShortNameUsesPegasusLiteForNonSharedFsJobs() throws Exception {
        TestGridStartFactory factory = configuredFactory(null);
        Job job = new Job();
        job.setDataConfiguration(PegasusConfiguration.NON_SHARED_FS_CONFIGURATION_VALUE);

        assertThat(factory.shortNameFor(job), is("PegasusLite"));
    }

    private TestGridStartFactory configuredFactory(String gridStart) throws Exception {
        TestGridStartFactory factory = new TestGridStartFactory();
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        if (gridStart != null) {
            properties.setProperty("pegasus.gridstart", gridStart);
        }
        ReflectionTestUtils.setField(factory, "mProps", properties);
        return factory;
    }
}
