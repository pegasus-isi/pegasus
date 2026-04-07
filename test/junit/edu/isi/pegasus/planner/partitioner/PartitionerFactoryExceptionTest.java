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
package edu.isi.pegasus.planner.partitioner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** @author Rajiv Mayani */
public class PartitionerFactoryExceptionTest {

    @Test
    public void testDefaultNameConstant() {
        assertThat(PartitionerFactoryException.DEFAULT_NAME, is("Partitioner"));
    }

    @Test
    public void testMessageOnlyConstructorUsesDefaultClassname() throws Exception {
        PartitionerFactoryException ex = new PartitionerFactoryException("msg");

        assertThat(ex.getMessage(), is("msg"));
        assertThat(
                ReflectionTestUtils.getField(ex, "mClassname"),
                is(PartitionerFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testMessageAndCauseConstructorUsesDefaultClassname() throws Exception {
        RuntimeException cause = new RuntimeException("root");
        PartitionerFactoryException ex = new PartitionerFactoryException("msg", cause);

        assertThat(ex.getCause(), is(sameInstance(cause)));
        assertThat(
                ReflectionTestUtils.getField(ex, "mClassname"),
                is(PartitionerFactoryException.DEFAULT_NAME));
    }

    @Test
    public void testExplicitClassnameAndCauseConstructorPreservesInputs() throws Exception {
        RuntimeException cause = new RuntimeException("root");
        PartitionerFactoryException ex =
                new PartitionerFactoryException("msg", "CustomPartitioner", cause);

        assertThat(ex.getMessage(), is("msg"));
        assertThat(ex.getCause(), is(sameInstance(cause)));
        assertThat(ReflectionTestUtils.getField(ex, "mClassname"), is("CustomPartitioner"));
    }
}
