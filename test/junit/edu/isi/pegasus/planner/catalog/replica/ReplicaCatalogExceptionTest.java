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
package edu.isi.pegasus.planner.catalog.replica;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.CatalogException;
import java.lang.reflect.Constructor;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class ReplicaCatalogExceptionTest {

    @Test
    public void testDefaultConstructorCreatesException() {
        ReplicaCatalogException ex = new ReplicaCatalogException();
        assertThat(ex, is(notNullValue()));
        assertThat(ex.getMessage(), is(nullValue()));
    }

    @Test
    public void testStringConstructorSetsMessage() {
        ReplicaCatalogException ex = new ReplicaCatalogException("replica error");
        assertThat(ex.getMessage(), is("replica error"));
    }

    @Test
    public void testStringCauseConstructorSetsBoth() {
        Throwable cause = new RuntimeException("root cause");
        ReplicaCatalogException ex = new ReplicaCatalogException("wrapper", cause);
        assertThat(ex.getMessage(), is("wrapper"));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testCauseOnlyConstructor() {
        Throwable cause = new RuntimeException("root cause");
        ReplicaCatalogException ex = new ReplicaCatalogException(cause);
        assertThat(ex.getCause(), is(cause));
        assertThat(ex.getMessage(), is(cause.toString()));
    }

    @Test
    public void testNullCauseOnlyConstructorLeavesMessageAndCauseNull() {
        ReplicaCatalogException ex = new ReplicaCatalogException((Throwable) null);

        assertThat(ex.getCause(), is(nullValue()));
        assertThat(ex.getMessage(), is(nullValue()));
    }

    @Test
    public void testIsInstanceOfCatalogException() {
        ReplicaCatalogException ex = new ReplicaCatalogException("test");
        assertThat(ex, instanceOf(CatalogException.class));
    }

    @Test
    public void testIsRuntimeException() {
        ReplicaCatalogException ex = new ReplicaCatalogException("test");
        assertThat(ex, instanceOf(RuntimeException.class));
    }

    @Test
    public void testExceptionCanBeThrown() {
        assertThrows(
                ReplicaCatalogException.class,
                () -> {
                    throw new ReplicaCatalogException("thrown");
                });
    }

    @Test
    public void testExceptionCaughtAsCatalogException() {
        assertThrows(
                CatalogException.class,
                () -> {
                    throw new ReplicaCatalogException("thrown as CatalogException");
                });
    }

    @Test
    public void testGetNextExceptionInherited() {
        ReplicaCatalogException first = new ReplicaCatalogException("first");
        ReplicaCatalogException second = new ReplicaCatalogException("second");
        first.setNextException(second);
        assertThat(first.getNextException(), is(second));
    }

    @Test
    public void testDefaultConstructorStartsWithNoNextException() {
        ReplicaCatalogException ex = new ReplicaCatalogException("first");

        assertThat(ex.getNextException(), is(nullValue()));
    }

    @Test
    public void testChainedExceptions() {
        ReplicaCatalogException root = new ReplicaCatalogException("root");
        root.setNextException(new ReplicaCatalogException("child1"));
        root.setNextException(new ReplicaCatalogException("child2"));

        int count = 0;
        for (CatalogException ex = root; ex != null; ex = ex.getNextException()) {
            count++;
        }
        assertThat(count, is(3));
    }

    @Test
    public void testSetNextExceptionAppendsToTailInInsertionOrder() {
        ReplicaCatalogException root = new ReplicaCatalogException("root");
        ReplicaCatalogException child1 = new ReplicaCatalogException("child1");
        ReplicaCatalogException child2 = new ReplicaCatalogException("child2");

        root.setNextException(child1);
        root.setNextException(child2);

        assertThat(root.getNextException(), is(child1));
        assertThat(child1.getNextException(), is(child2));
        assertThat(child2.getNextException(), is(nullValue()));
    }

    @Test
    public void testStringCauseConstructorWithNullsPreservesNulls() {
        ReplicaCatalogException ex = new ReplicaCatalogException((String) null, (Throwable) null);

        assertThat(ex.getMessage(), is(nullValue()));
        assertThat(ex.getCause(), is(nullValue()));
    }

    @Test
    public void testExactSuperclassIsCatalogException() {
        assertThat(ReplicaCatalogException.class.getSuperclass(), is(CatalogException.class));
    }

    @Test
    public void testDeclaresExpectedFourConstructors() {
        Constructor<?>[] constructors = ReplicaCatalogException.class.getDeclaredConstructors();

        assertThat(constructors.length, is(4));
    }
}
