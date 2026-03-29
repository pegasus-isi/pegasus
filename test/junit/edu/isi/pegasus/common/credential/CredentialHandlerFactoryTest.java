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
package edu.isi.pegasus.common.credential;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Structural tests for the CredentialHandlerFactory class via reflection.
 *
 * @author Rajiv Mayani
 */
public class CredentialHandlerFactoryTest {

    private CredentialHandlerFactory factory;

    @BeforeEach
    public void setUp() {
        factory = new CredentialHandlerFactory();
    }

    // --- class structure ---

    @Test
    public void testCredentialHandlerFactoryIsConcreteClass() {
        assertFalse(Modifier.isAbstract(CredentialHandlerFactory.class.getModifiers()));
    }

    @Test
    public void testDefaultPackageNameConstant() {
        assertThat(
                CredentialHandlerFactory.DEFAULT_PACKAGE_NAME,
                is("edu.isi.pegasus.common.credential.impl"));
    }

    // --- loadInstance before initialize ---

    @Test
    public void testLoadInstanceThrowsWhenNotInitialized() {
        assertThrows(
                CredentialHandlerFactoryException.class,
                () -> factory.loadInstance(CredentialHandler.TYPE.x509));
    }

    // --- implementingClassNameTable (via reflection) ---

    @Test
    public void testAllExpectedTypesAreMapped() throws Exception {
        Map<CredentialHandler.TYPE, String> table = getImplementingClassNameTable();
        assertThat(
                table.keySet(),
                hasItems(
                        CredentialHandler.TYPE.credentials,
                        CredentialHandler.TYPE.http,
                        CredentialHandler.TYPE.x509,
                        CredentialHandler.TYPE.irods,
                        CredentialHandler.TYPE.boto,
                        CredentialHandler.TYPE.googlep12,
                        CredentialHandler.TYPE.ssh));
    }

    @Test
    public void testS3TypeIsNotMapped() throws Exception {
        // s3 was intentionally removed (PM-1831): transfers now rely on credentials.conf
        Map<CredentialHandler.TYPE, String> table = getImplementingClassNameTable();
        assertThat(table.keySet(), not(hasItem(CredentialHandler.TYPE.s3)));
    }

    @Test
    public void testAllMappedClassNamesAreNonEmpty() throws Exception {
        Map<CredentialHandler.TYPE, String> table = getImplementingClassNameTable();
        assertFalse(table.isEmpty());
        for (Map.Entry<CredentialHandler.TYPE, String> entry : table.entrySet()) {
            assertThat(
                    "class name for type " + entry.getKey() + " must not be empty",
                    entry.getValue(),
                    not(emptyString()));
        }
    }

    @Test
    public void testAllMappedClassNamesAreSimpleBaseNames() throws Exception {
        // the factory prepends DEFAULT_PACKAGE_NAME at load time; the table holds only basenames
        Map<CredentialHandler.TYPE, String> table = getImplementingClassNameTable();
        for (Map.Entry<CredentialHandler.TYPE, String> entry : table.entrySet()) {
            assertThat(
                    "class name for type " + entry.getKey() + " should be a simple basename",
                    entry.getValue(),
                    not(containsString(".")));
        }
    }

    // --- helper ---

    @SuppressWarnings("unchecked")
    private Map<CredentialHandler.TYPE, String> getImplementingClassNameTable() throws Exception {
        Method m = CredentialHandlerFactory.class.getDeclaredMethod("implementingClassNameTable");
        m.setAccessible(true);
        return (Map<CredentialHandler.TYPE, String>) m.invoke(null);
    }
}
