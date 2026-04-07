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
package edu.isi.pegasus.planner.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class AuthenticateRequestTest {

    @Test
    public void testConstructorAndGetters() {
        AuthenticateRequest request =
                new AuthenticateRequest(
                        AuthenticateRequest.GRIDFTP_RESOURCE, "poolA", "gsiftp://example/path");

        assertThat(request.getResourceType(), is(AuthenticateRequest.GRIDFTP_RESOURCE));
        assertThat(request.getPool(), is("poolA"));
        assertThat(request.getResourceContact(), is("gsiftp://example/path"));
    }

    @Test
    public void testToStringUsesCurrentFieldFormat() {
        AuthenticateRequest request =
                new AuthenticateRequest(
                        AuthenticateRequest.JOBMANAGER_RESOURCE,
                        "submit",
                        "host.example/jobmanager");

        assertThat(request.toString(), is("TYPE-->j  Pool-->submit URL-->host.example/jobmanager"));
    }

    @Test
    public void testCloneReturnsDistinctCopyWithSameValues() {
        AuthenticateRequest original =
                new AuthenticateRequest(
                        AuthenticateRequest.GRIDFTP_RESOURCE, "poolA", "gsiftp://example/path");

        AuthenticateRequest clone = (AuthenticateRequest) original.clone();

        assertThat(clone, is(not(sameInstance(original))));
        assertThat(clone.getResourceType(), is(original.getResourceType()));
        assertThat(clone.getPool(), is(original.getPool()));
        assertThat(clone.getResourceContact(), is(original.getResourceContact()));
    }

    @Test
    public void testRequestInvalidRejectsEmptyContactAndUnknownType() {
        AuthenticateRequest emptyContact =
                new AuthenticateRequest(AuthenticateRequest.GRIDFTP_RESOURCE, "poolA", "");
        AuthenticateRequest unknownType =
                new AuthenticateRequest('x', "poolA", "resource://contact");

        assertThat(emptyContact.requestInvalid(), is(true));
        assertThat(unknownType.requestInvalid(), is(true));
    }

    @Test
    public void testRequestInvalidAcceptsSupportedResourceTypes() {
        AuthenticateRequest gridftp =
                new AuthenticateRequest(
                        AuthenticateRequest.GRIDFTP_RESOURCE, "poolA", "gsiftp://example/path");
        AuthenticateRequest jobmanager =
                new AuthenticateRequest(
                        AuthenticateRequest.JOBMANAGER_RESOURCE,
                        "poolB",
                        "host.example/jobmanager");

        assertThat(gridftp.requestInvalid(), is(false));
        assertThat(jobmanager.requestInvalid(), is(false));
    }
}
