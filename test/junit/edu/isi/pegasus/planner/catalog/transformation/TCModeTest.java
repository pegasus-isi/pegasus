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
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import org.junit.jupiter.api.Test;

/** Tests for the TCMode class constants. */
public class TCModeTest {

    @Test
    public void testSingleReadConstantValue() {
        assertThat(TCMode.SINGLE_READ, is("single"));
    }

    @Test
    public void testMultipleReadConstantValue() {
        assertThat(TCMode.MULTIPLE_READ, is("multiple"));
    }

    @Test
    public void testOldFileTCClassConstantValue() {
        assertThat(TCMode.OLDFILE_TC_CLASS, is("OldFile"));
    }

    @Test
    public void testDefaultTCClassConstantValue() {
        assertThat(TCMode.DEFAULT_TC_CLASS, is("File"));
    }

    @Test
    public void testPackageNameConstantIsNonEmpty() {
        assertThat(TCMode.PACKAGE_NAME, is(notNullValue()));
        assertThat(TCMode.PACKAGE_NAME.isEmpty(), is(false));
    }

    @Test
    public void testSingleReadIsDistinctFromMultipleRead() {
        assertNotEquals(TCMode.SINGLE_READ, TCMode.MULTIPLE_READ);
    }

    @Test
    public void testOldFileTCClassIsDistinctFromDefaultTCClass() {
        assertNotEquals(TCMode.OLDFILE_TC_CLASS, TCMode.DEFAULT_TC_CLASS);
    }

    @Test
    public void testGetImplementingClassMapsSingleReadToOldFile() throws Exception {
        assertThat(invokeGetImplementingClass("single"), is(TCMode.OLDFILE_TC_CLASS));
    }

    @Test
    public void testGetImplementingClassMapsMultipleReadCaseInsensitivelyToOldFile()
            throws Exception {
        assertThat(invokeGetImplementingClass("  MuLtIpLe  "), is(TCMode.OLDFILE_TC_CLASS));
    }

    @Test
    public void testGetImplementingClassReturnsCustomClassNameWhenNotLegacyMode() throws Exception {
        assertThat(invokeGetImplementingClass("CustomTC"), is("CustomTC"));
    }

    @Test
    public void testGetImplementingClassReturnsDefaultClassNameUnchanged() throws Exception {
        assertThat(
                invokeGetImplementingClass(TCMode.DEFAULT_TC_CLASS), is(TCMode.DEFAULT_TC_CLASS));
    }

    private String invokeGetImplementingClass(String value) throws Exception {
        Method method = TCMode.class.getDeclaredMethod("getImplementingClass", String.class);
        method.setAccessible(true);
        return (String) method.invoke(null, value);
    }
}
