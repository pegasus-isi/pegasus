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
package edu.isi.pegasus.planner.mapper.output;

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.classes.NameValue;
import edu.isi.pegasus.planner.mapper.OutputMapper;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the Fixed output mapper class structure. */
public class FixedTest {

    @Test
    public void testFixedImplementsOutputMapper() {
        org.hamcrest.MatcherAssert.assertThat(
                OutputMapper.class.isAssignableFrom(Fixed.class), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testPropertyPrefixConstant() {
        org.hamcrest.MatcherAssert.assertThat(
                Fixed.PROPERTY_PREFIX,
                org.hamcrest.Matchers.is("pegasus.dir.storage.mapper.fixed"));
    }

    @Test
    public void testDefaultInstantiation() {
        Fixed fixed = new Fixed();
        org.hamcrest.MatcherAssert.assertThat(fixed, org.hamcrest.Matchers.notNullValue());
    }

    @Test
    public void testFixedIsPublicClass() {
        int modifiers = Fixed.class.getModifiers();
        org.hamcrest.MatcherAssert.assertThat(
                java.lang.reflect.Modifier.isPublic(modifiers), org.hamcrest.Matchers.is(true));
    }

    @Test
    public void testFixedDoesNotExtendAbstractFileFactoryBasedMapper() {
        // Fixed implements OutputMapper directly, not through AbstractFileFactoryBasedMapper
        org.hamcrest.MatcherAssert.assertThat(
                AbstractFileFactoryBasedMapper.class.isAssignableFrom(Fixed.class),
                org.hamcrest.Matchers.is(false));
    }

    @Test
    public void testDescriptionReturnsExpectedText() {
        org.hamcrest.MatcherAssert.assertThat(
                new Fixed().description(), org.hamcrest.Matchers.is("Fixed Directory mapper"));
    }

    @Test
    public void testMapBuildsUrlFromFixedDirectory() throws Exception {
        Fixed fixed = new Fixed();
        setDirectoryURL(fixed, "file:///outputs");

        NameValue<String, String> mapped = fixed.map("f.out", "local", FileServer.OPERATION.put);

        org.hamcrest.MatcherAssert.assertThat(mapped.getKey(), org.hamcrest.Matchers.is("local"));
        org.hamcrest.MatcherAssert.assertThat(
                mapped.getValue(), org.hamcrest.Matchers.is("file:///outputs/f.out"));
    }

    @Test
    public void testMapAllReturnsSingletonList() throws Exception {
        Fixed fixed = new Fixed();
        setDirectoryURL(fixed, "file:///outputs");

        List<NameValue<String, String>> mapped =
                fixed.mapAll("f.out", "local", FileServer.OPERATION.get);

        org.hamcrest.MatcherAssert.assertThat(mapped.size(), org.hamcrest.Matchers.is(1));
        org.hamcrest.MatcherAssert.assertThat(
                mapped.get(0).getValue(), org.hamcrest.Matchers.is("file:///outputs/f.out"));
    }

    @Test
    public void testGetErrorMessagePrefixUsesShortName() {
        TestFixed fixed = new TestFixed();

        org.hamcrest.MatcherAssert.assertThat(
                fixed.errorPrefix(), org.hamcrest.Matchers.is("[Fixed] "));
    }

    private static void setDirectoryURL(Fixed fixed, String value) throws Exception {
        ReflectionTestUtils.setField(fixed, "mDirectoryURL", value);
    }

    private static class TestFixed extends Fixed {
        String errorPrefix() {
            return getErrorMessagePrefix();
        }
    }
}
