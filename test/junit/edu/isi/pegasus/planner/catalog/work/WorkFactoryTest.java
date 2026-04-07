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
package edu.isi.pegasus.planner.catalog.work;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.util.CommonProperties;
import edu.isi.pegasus.common.util.FactoryException;
import edu.isi.pegasus.planner.catalog.WorkCatalog;
import edu.isi.pegasus.planner.common.PegasusProperties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class WorkFactoryTest {

    @Test
    public void testDefaultPackageConstant() {
        assertThat(WorkFactory.DEFAULT_PACKAGE, equalTo("edu.isi.pegasus.planner.catalog.work"));
    }

    @Test
    public void testLoadInstanceRejectsNullCommonProperties() {
        NullPointerException exception =
                assertThrows(
                        NullPointerException.class,
                        () -> WorkFactory.loadInstance((CommonProperties) null));

        assertThat(exception.getMessage(), is("invalid properties"));
    }

    @Test
    public void testLoadInstanceRejectsMissingCatalogImplementorProperty() throws Exception {
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();

        WorkFactoryException exception =
                assertThrows(
                        WorkFactoryException.class, () -> WorkFactory.loadInstance(properties));

        assertThat(getClassname(exception), nullValue());
        assertThat(exception.getCause(), is(notNullValue()));
        assertThat(exception.getCause().getMessage(), containsString(WorkCatalog.c_prefix));
    }

    @Test
    public void testLoadInstanceAcceptsShortClassNameAndPrefixesDefaultPackage() throws Exception {
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        properties.setProperty(WorkCatalog.c_prefix, "Database");

        WorkFactoryException exception =
                assertThrows(
                        WorkFactoryException.class, () -> WorkFactory.loadInstance(properties));

        assertThat(
                getClassname(exception), equalTo("edu.isi.pegasus.planner.catalog.work.Database"));
    }

    @Test
    public void testLoadInstanceAcceptsFullyQualifiedClassName() throws Exception {
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        properties.setProperty(
                WorkCatalog.c_prefix, "edu.isi.pegasus.planner.catalog.work.Database");

        WorkFactoryException exception =
                assertThrows(
                        WorkFactoryException.class, () -> WorkFactory.loadInstance(properties));

        assertThat(
                getClassname(exception), equalTo("edu.isi.pegasus.planner.catalog.work.Database"));
    }

    private static String getClassname(FactoryException exception) {
        return (String) ReflectionTestUtils.getField(exception, "mClassname");
    }
}
