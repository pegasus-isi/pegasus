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

import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.logger.Default;
import edu.isi.pegasus.common.logging.logger.Log4j;
import java.lang.reflect.Modifier;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the LogManagerFactory class. */
public class LogManagerFactoryTest {

    @Test
    public void testLogManagerFactoryIsConcreteClass() {
        assertFalse(
                Modifier.isAbstract(LogManagerFactory.class.getModifiers()),
                "LogManagerFactory should be a concrete class");
    }

    @Test
    public void testDefaultPackageNameConstant() {
        assertEquals(
                "edu.isi.pegasus.common.logging.logger",
                LogManagerFactory.DEFAULT_PACKAGE_NAME,
                "DEFAULT_PACKAGE_NAME should be 'edu.isi.pegasus.common.logging.logger'");
    }

    @Test
    public void testDefaultPackageNameIsNotEmpty() {
        assertFalse(
                LogManagerFactory.DEFAULT_PACKAGE_NAME.isEmpty(),
                "DEFAULT_PACKAGE_NAME should not be empty");
    }

    @Test
    public void testLoadInstanceDefaultLogger() {
        LogManager manager = LogManagerFactory.loadInstance("Default", "Simple", new Properties());
        assertNotNull(manager, "loadInstance('Default') should return a non-null LogManager");
        assertTrue(
                manager instanceof Default,
                "loadInstance('Default') should return a Default instance");
    }

    @Test
    public void testLoadInstanceLog4jLogger() {
        LogManager manager = LogManagerFactory.loadInstance("Log4j", "Simple", new Properties());
        assertNotNull(manager, "loadInstance('Log4j') should return a non-null LogManager");
        assertTrue(
                manager instanceof Log4j, "loadInstance('Log4j') should return a Log4j instance");
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
        assertEquals(0, manager.getLevel(), "Default LogManager should start at level 0");
    }
}
