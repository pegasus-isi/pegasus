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

/** Tests for POSTScript interface structure */
public class POSTScriptTest {

    @Test
    public void testVersionConstant() {
        assertEquals("1.1", POSTScript.VERSION);
    }

    @Test
    public void testInterfaceIsPublic() {
        assertTrue(java.lang.reflect.Modifier.isPublic(POSTScript.class.getModifiers()));
    }

    @Test
    public void testInterfaceHasInitializeMethod() throws NoSuchMethodException {
        assertNotNull(
                POSTScript.class.getMethod(
                        "initialize",
                        edu.isi.pegasus.planner.common.PegasusProperties.class,
                        String.class,
                        String.class,
                        String.class));
    }

    @Test
    public void testInterfaceHasConstructMethod() throws NoSuchMethodException {
        assertNotNull(
                POSTScript.class.getMethod(
                        "construct", edu.isi.pegasus.planner.classes.Job.class, String.class));
    }

    @Test
    public void testInterfaceHasShortDescribeMethod() throws NoSuchMethodException {
        assertNotNull(POSTScript.class.getMethod("shortDescribe"));
    }

    @Test
    public void testNoPOSTScriptImplementsPOSTScript() {
        assertTrue(
                POSTScript.class.isAssignableFrom(
                        edu.isi.pegasus.planner.code.gridstart.NoPOSTScript.class));
    }

    @Test
    public void testUserPOSTScriptImplementsPOSTScript() {
        assertTrue(
                POSTScript.class.isAssignableFrom(
                        edu.isi.pegasus.planner.code.gridstart.UserPOSTScript.class));
    }
}
