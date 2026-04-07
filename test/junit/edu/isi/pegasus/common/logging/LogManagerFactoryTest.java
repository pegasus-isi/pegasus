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
package edu.isi.pegasus.common.logging;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.logger.Default;
import edu.isi.pegasus.common.logging.logger.Log4j;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.lang.reflect.Modifier;
import java.util.Properties;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the LogManagerFactory class. */
public class LogManagerFactoryTest {

    @Test
    public void testLogManagerFactoryIsConcreteClass() {
        assertThat(Modifier.isAbstract(LogManagerFactory.class.getModifiers()), is(false));
    }

    @Test
    public void testDefaultPackageNameConstant() {
        assertThat(
                LogManagerFactory.DEFAULT_PACKAGE_NAME,
                is("edu.isi.pegasus.common.logging.logger"));
    }

    @Test
    public void testDefaultPackageNameIsNotEmpty() {
        assertThat(LogManagerFactory.DEFAULT_PACKAGE_NAME.isEmpty(), is(false));
    }

    @Test
    public void testLoadInstanceDefaultLogger() {
        LogManager manager = LogManagerFactory.loadInstance("Default", "Simple", new Properties());
        assertThat(manager, is(notNullValue()));
        assertThat(manager, instanceOf(Default.class));
    }

    @Test
    public void testLoadInstanceLog4jLogger() {
        LogManager manager = LogManagerFactory.loadInstance("Log4j", "Simple", new Properties());
        assertThat(manager, is(notNullValue()));
        assertThat(manager, instanceOf(Log4j.class));
    }

    @Test
    public void testLoadInstanceThrowsExceptionForUnknownClass() {
        assertThrows(
                LogManagerFactoryException.class,
                () -> LogManagerFactory.loadInstance("NonExistent", "Simple", new Properties()),
                "loadInstance for unknown class should throw LogManagerFactoryException");
    }

    @Test
    public void testLoadedManagerHasCorrectDefaultLevel() {
        LogManager manager = LogManagerFactory.loadInstance("Default", "Simple", new Properties());
        // LogManager initializes mDebugLevel=0 regardless of implementation
        assertThat(manager.getLevel(), is(0));
    }

    @Test
    public void testLoadInstanceThrowsForNullPropertiesOverload() {
        assertThrows(
                LogManagerFactoryException.class,
                () -> LogManagerFactory.loadInstance((PegasusProperties) null));
    }

    @Test
    public void testLoadSingletonInstanceReturnsSeededSingleton() throws Exception {
        LogManager seeded = new Default();
        ReflectionTestUtils.setField(LogManagerFactory.class, "mSingletonInstance", seeded);

        LogManager result =
                LogManagerFactory.loadSingletonInstance(PegasusProperties.nonSingletonInstance());

        assertThat(result, sameInstance(seeded));
    }

    @Test
    public void testPropertiesOverloadMethodSignature() throws Exception {
        assertThat(
                LogManagerFactory.class
                        .getMethod("loadInstance", PegasusProperties.class)
                        .getReturnType(),
                is(LogManager.class));
    }
}
