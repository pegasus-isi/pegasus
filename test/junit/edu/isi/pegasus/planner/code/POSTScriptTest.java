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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for POSTScript interface structure */
public class POSTScriptTest {

    @Test
    public void testVersionConstant() {
        assertThat(POSTScript.VERSION, is("1.1"));
    }

    @Test
    public void testInterfaceIsPublic() {
        assertThat(java.lang.reflect.Modifier.isPublic(POSTScript.class.getModifiers()), is(true));
    }

    @Test
    public void testInterfaceHasInitializeMethod() throws NoSuchMethodException {
        assertThat(
                POSTScript.class.getMethod(
                        "initialize",
                        edu.isi.pegasus.planner.common.PegasusProperties.class,
                        String.class,
                        String.class,
                        String.class),
                notNullValue());
    }

    @Test
    public void testInterfaceHasConstructMethod() throws NoSuchMethodException {
        assertThat(
                POSTScript.class.getMethod(
                        "construct", edu.isi.pegasus.planner.classes.Job.class, String.class),
                notNullValue());
    }

    @Test
    public void testInterfaceHasShortDescribeMethod() throws NoSuchMethodException {
        assertThat(POSTScript.class.getMethod("shortDescribe"), notNullValue());
    }

    @Test
    public void testNoPOSTScriptImplementsPOSTScript() {
        assertThat(
                POSTScript.class.isAssignableFrom(
                        edu.isi.pegasus.planner.code.gridstart.NoPOSTScript.class),
                is(true));
    }

    @Test
    public void testUserPOSTScriptImplementsPOSTScript() {
        assertThat(
                POSTScript.class.isAssignableFrom(
                        edu.isi.pegasus.planner.code.gridstart.UserPOSTScript.class),
                is(true));
    }

    @Test
    public void testTypeIsAnInterface() {
        assertThat(POSTScript.class.isInterface(), is(true));
    }

    @Test
    public void testVersionConstantIsNotEmpty() {
        assertThat(POSTScript.VERSION.isEmpty(), is(false));
    }

    @Test
    public void testInterfaceDeclaresExpectedMethodNames() {
        List<String> methodNames = Arrays.asList("initialize", "construct", "shortDescribe");

        for (String methodName : methodNames) {
            assertThat(
                    Arrays.stream(POSTScript.class.getDeclaredMethods())
                            .map(Method::getName)
                            .anyMatch(methodName::equals),
                    is(true));
        }
    }
}
