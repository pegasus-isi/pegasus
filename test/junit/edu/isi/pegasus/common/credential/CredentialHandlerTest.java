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

import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PegasusBag;
import org.junit.jupiter.api.Test;

/**
 * Structural tests for the CredentialHandler interface via reflection.
 *
 * @author Rajiv Mayani
 */
public class CredentialHandlerTest {

    // --- interface structure ---

    @Test
    public void testCredentialHandlerIsInterface() {
        assertTrue(CredentialHandler.class.isInterface());
    }

    // --- VERSION constant ---

    @Test
    public void testVersionConstant() {
        assertThat(CredentialHandler.VERSION, is("1.5"));
    }

    // --- TYPE enum ---

    @Test
    public void testTypeEnumContainsAllValues() {
        assertThat(
                CredentialHandler.TYPE.values(),
                arrayContainingInAnyOrder(
                        CredentialHandler.TYPE.credentials,
                        CredentialHandler.TYPE.http,
                        CredentialHandler.TYPE.x509,
                        CredentialHandler.TYPE.s3,
                        CredentialHandler.TYPE.boto,
                        CredentialHandler.TYPE.googlep12,
                        CredentialHandler.TYPE.irods,
                        CredentialHandler.TYPE.ssh));
    }

    // --- method declarations (NoSuchMethodException = test failure if missing) ---

    @Test
    public void testNoArgMethods() throws NoSuchMethodException {
        CredentialHandler.class.getMethod("getPath");
        CredentialHandler.class.getMethod("getProfileKey");
        CredentialHandler.class.getMethod("getDescription");
    }

    @Test
    public void testSiteArgMethods() throws NoSuchMethodException {
        CredentialHandler.class.getMethod("getPath", String.class);
        CredentialHandler.class.getMethod("getEnvironmentVariable", String.class);
        CredentialHandler.class.getMethod("getBaseName", String.class);
    }

    @Test
    public void testInitializeMethod() throws NoSuchMethodException {
        CredentialHandler.class.getMethod("initialize", PegasusBag.class);
    }

    @Test
    public void testVerifyMethod() throws NoSuchMethodException {
        CredentialHandler.class.getMethod(
                "verify", Job.class, CredentialHandler.TYPE.class, String.class);
    }

    @Test
    public void testHasCredentialMethod() throws NoSuchMethodException {
        CredentialHandler.class.getMethod(
                "hasCredential", CredentialHandler.TYPE.class, String.class, String.class);
    }
}
