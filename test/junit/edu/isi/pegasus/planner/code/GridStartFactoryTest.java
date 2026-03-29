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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** Tests for GridStartFactory constants and structure */
public class GridStartFactoryTest {

    @Test
    public void testDefaultPackageName() {
        assertEquals(
                "edu.isi.pegasus.planner.code.gridstart", GridStartFactory.DEFAULT_PACKAGE_NAME);
    }

    @Test
    public void testDefaultGridstartMode() {
        assertEquals("Kickstart", GridStartFactory.DEFAULT_GRIDSTART_MODE);
    }

    @Test
    public void testKickstartIndex() {
        assertEquals(0, GridStartFactory.KICKSTART_INDEX);
    }

    @Test
    public void testNoGridstartIndex() {
        assertEquals(1, GridStartFactory.NO_GRIDSTART_INDEX);
    }

    @Test
    public void testGridstartShortNamesLength() {
        assertEquals(2, GridStartFactory.GRIDSTART_SHORT_NAMES.length);
        assertEquals("kickstart", GridStartFactory.GRIDSTART_SHORT_NAMES[0]);
        assertEquals("none", GridStartFactory.GRIDSTART_SHORT_NAMES[1]);
    }

    @Test
    public void testGridstartImplementingClassesLength() {
        assertEquals(2, GridStartFactory.GRIDSTART_IMPLEMENTING_CLASSES.length);
        assertEquals("Kickstart", GridStartFactory.GRIDSTART_IMPLEMENTING_CLASSES[0]);
        assertEquals("NoGridStart", GridStartFactory.GRIDSTART_IMPLEMENTING_CLASSES[1]);
    }

    @Test
    public void testPostScriptScopeConstants() {
        assertEquals("essential", GridStartFactory.ESSENTIAL_POST_SCRIPT_SCOPE);
        assertEquals("all", GridStartFactory.ALL_POST_SCRIPT_SCOPE);
    }
}
