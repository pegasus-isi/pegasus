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
package edu.isi.pegasus.planner.cluster.aggregator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.cluster.JobAggregator;
import org.junit.jupiter.api.Test;

/** Tests for the Decaf aggregator class. */
public class DecafTest {

    @Test
    public void testDecafExtendsAbstract() {
        assertThat(Abstract.class.isAssignableFrom(Decaf.class), is(true));
    }

    @Test
    public void testDecafImplementsJobAggregator() {
        assertThat(JobAggregator.class.isAssignableFrom(Decaf.class), is(true));
    }

    @Test
    public void testTransformationNamespaceConstant() {
        assertThat(Decaf.TRANSFORMATION_NAMESPACE, is("dataflow"));
    }

    @Test
    public void testTransformationNameConstant() {
        assertThat(Decaf.TRANSFORMATION_NAME, is("decaf"));
    }

    @Test
    public void testDecafExtraArgsSelectorProfileKeyConstant() {
        assertThat(Decaf.DECAF_EXTRA_ARGS_SELECTOR_PROFILE_KEY, is("decaf.args"));
    }

    @Test
    public void testDefaultInstantiation() {
        Decaf decaf = new Decaf();
        assertThat(decaf, notNullValue());
    }

    @Test
    public void testTopologicalOrderingIsNotRequired() {
        Decaf decaf = new Decaf();

        assertThat(decaf.topologicalOrderingRequired(), is(false));
    }

    @Test
    public void testEntryNotInTCAlwaysReturnsFalse() {
        Decaf decaf = new Decaf();

        assertThat(decaf.entryNotInTC("local"), is(false));
    }

    @Test
    public void testGetClusterExecutableLFNReturnsDecaf() {
        Decaf decaf = new Decaf();

        assertThat(decaf.getClusterExecutableLFN(), is("decaf"));
    }

    @Test
    public void testGetClusterExecutableBasenameThrows() {
        Decaf decaf = new Decaf();
        RuntimeException exception =
                assertThrows(RuntimeException.class, decaf::getClusterExecutableBasename);

        assertThat(
                exception
                        .getMessage()
                        .contains("does not create default transformation catalog entries"),
                is(true));
        assertThat(exception.getMessage().contains("dataflow,decaf"), is(true));
    }

    @Test
    public void testSetAbortOnFirstJobFailureIsUnsupported() {
        Decaf decaf = new Decaf();

        assertThrows(
                UnsupportedOperationException.class, () -> decaf.setAbortOnFirstJobFailure(true));
    }

    @Test
    public void testAbortOnFirstJobFailureAccessorIsUnsupported() {
        Decaf decaf = new Decaf();

        assertThrows(UnsupportedOperationException.class, decaf::abortOnFristJobFailure);
    }
}
