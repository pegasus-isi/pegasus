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
package edu.isi.pegasus.planner.namespace;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class TestNamespaceTest {

    @Test
    public void pegasusAndCondorExposeExpectedNamespaceNames() {
        Pegasus pegasus = new Pegasus();
        Condor condor = new Condor();

        assertThat(pegasus.namespaceName(), is(Pegasus.NAMESPACE_NAME));
        assertThat(condor.namespaceName(), is(Condor.NAMESPACE_NAME));
    }

    @Test
    public void pegasusDeprecatedTableContainsKnownMappings() {
        Map deprecated = new Pegasus().deprecatedTable();

        assertThat(
                deprecated.get(Pegasus.DEPRECATED_BUNDLE_STAGE_IN_KEY),
                is(Pegasus.BUNDLE_STAGE_IN_KEY));
        assertThat(deprecated.get(Pegasus.DEPRECATED_CHANGE_DIR_KEY), is(Pegasus.CHANGE_DIR_KEY));
    }

    @Test
    public void condorDeprecatedTableIsUnsupported() {
        UnsupportedOperationException exception =
                assertThrows(
                        UnsupportedOperationException.class, () -> new Condor().deprecatedTable());

        assertThat(exception.getMessage(), containsString(Condor.NAMESPACE_NAME));
    }
}
