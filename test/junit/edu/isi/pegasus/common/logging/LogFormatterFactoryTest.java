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

import edu.isi.pegasus.common.logging.format.Netlogger;
import edu.isi.pegasus.common.logging.format.Simple;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Tests for the LogFormatterFactory class. */
public class LogFormatterFactoryTest {

    // --- class structure ---

    @Test
    public void testLogFormatterFactoryIsConcreteClass() {
        assertFalse(Modifier.isAbstract(LogFormatterFactory.class.getModifiers()));
    }

    // --- DEFAULT_PACKAGE_NAME constant ---

    @Test
    public void testDefaultPackageNameConstant() {
        assertThat(
                LogFormatterFactory.DEFAULT_PACKAGE_NAME,
                is("edu.isi.pegasus.common.logging.format"));
    }

    // --- loadInstance(String) by short name ---

    @Test
    public void testLoadInstanceNetloggerByShortName() {
        LogFormatter formatter = LogFormatterFactory.loadInstance("Netlogger");
        assertThat(formatter, instanceOf(Netlogger.class));
    }

    @Test
    public void testLoadInstanceSimpleByShortName() {
        LogFormatter formatter = LogFormatterFactory.loadInstance("Simple");
        assertThat(formatter, instanceOf(Simple.class));
    }

    // --- loadInstance(String) by fully qualified name ---

    @Test
    public void testLoadInstanceNetloggerByFullyQualifiedName() {
        LogFormatter formatter =
                LogFormatterFactory.loadInstance("edu.isi.pegasus.common.logging.format.Netlogger");
        assertThat(formatter, instanceOf(Netlogger.class));
    }

    @Test
    public void testLoadInstanceSimpleByFullyQualifiedName() {
        LogFormatter formatter =
                LogFormatterFactory.loadInstance("edu.isi.pegasus.common.logging.format.Simple");
        assertThat(formatter, instanceOf(Simple.class));
    }

    // --- loadInstance(String) error cases ---

    @Test
    public void testLoadInstanceThrowsForUnknownClass() {
        assertThrows(
                LogFormatterFactoryException.class,
                () -> LogFormatterFactory.loadInstance("NonExistentFormatter"));
    }

    @Test
    public void testLoadInstanceThrowsForNullImplementor() {
        assertThrows(
                LogFormatterFactoryException.class,
                () -> LogFormatterFactory.loadInstance((String) null));
    }

    // --- loadInstance(PegasusProperties) error cases ---

    @Test
    public void testLoadInstanceThrowsForNullProperties() {
        assertThrows(
                LogFormatterFactoryException.class,
                () -> LogFormatterFactory.loadInstance((String) null));
    }

    // --- loadSingletonInstance ---

    @Test
    public void testLoadSingletonInstanceReturnsLogFormatter() {
        LogFormatter formatter = LogFormatterFactory.loadSingletonInstance("Simple");
        assertThat(formatter, instanceOf(LogFormatter.class));
    }
}
