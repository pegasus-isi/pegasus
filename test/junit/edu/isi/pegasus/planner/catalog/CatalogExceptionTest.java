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
package edu.isi.pegasus.planner.catalog;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class CatalogExceptionTest {

    @Test
    public void testDefaultConstructorCreatesException() {
        CatalogException ex = new CatalogException();
        assertThat(ex, is(notNullValue()));
        assertThat(ex.getMessage(), is(nullValue()));
        assertThat(ex.getNextException(), is(nullValue()));
    }

    @Test
    public void testStringConstructorSetsMessage() {
        CatalogException ex = new CatalogException("test error");
        assertThat(ex.getMessage(), equalTo("test error"));
    }

    @Test
    public void testStringCauseConstructorSetsBoth() {
        Throwable cause = new RuntimeException("root cause");
        CatalogException ex = new CatalogException("wrapper message", cause);
        assertThat(ex.getMessage(), equalTo("wrapper message"));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testCauseOnlyConstructor() {
        Throwable cause = new RuntimeException("root cause");
        CatalogException ex = new CatalogException(cause);
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testGetNextExceptionInitiallyNull() {
        CatalogException ex = new CatalogException("first");
        assertThat(ex.getNextException(), is(nullValue()));
    }

    @Test
    public void testSetNextExceptionChainsSingle() {
        CatalogException first = new CatalogException("first");
        CatalogException second = new CatalogException("second");
        first.setNextException(second);
        assertThat(first.getNextException(), is(second));
    }

    @Test
    public void testSetNextExceptionChainsMultiple() {
        CatalogException first = new CatalogException("first");
        CatalogException second = new CatalogException("second");
        CatalogException third = new CatalogException("third");

        first.setNextException(second);
        first.setNextException(third); // should be appended to end of chain

        assertThat(first.getNextException(), is(second));
        assertThat(first.getNextException().getNextException(), is(third));
    }

    @Test
    public void testExceptionChainIterationPattern() {
        CatalogException root = new CatalogException("first");
        root.setNextException(new CatalogException("second"));
        root.setNextException(new CatalogException("third"));

        int count = 0;
        for (CatalogException rce = root; rce != null; rce = rce.getNextException()) {
            count++;
        }
        assertThat(count, is(3));
    }

    @Test
    public void testSetNextExceptionOnSecondPositionAppends() {
        CatalogException first = new CatalogException("first");
        CatalogException second = new CatalogException("second");
        first.setNextException(second);

        CatalogException third = new CatalogException("third");
        first.setNextException(third);

        // third should be at the end of the chain
        assertThat(third.getNextException(), is(nullValue()));
        assertThat(second.getNextException(), is(third));
    }

    @Test
    public void testStringConstructorWithNullMessage() {
        CatalogException ex = new CatalogException((String) null);
        assertThat(ex.getMessage(), is(nullValue()));
        assertThat(ex.getNextException(), is(nullValue()));
    }

    @Test
    public void testCauseConstructorWithNullCause() {
        CatalogException ex = new CatalogException((Throwable) null);
        assertThat(ex.getCause(), is(nullValue()));
    }

    @Test
    public void testStringCauseConstructorWithNullBoth() {
        CatalogException ex = new CatalogException((String) null, (Throwable) null);
        assertThat(ex.getMessage(), is(nullValue()));
        assertThat(ex.getCause(), is(nullValue()));
    }

    @Test
    public void testSetNextException_nullOnEmptyChain_isNoOp() {
        CatalogException ex = new CatalogException("first");
        ex.setNextException(null);
        assertThat(ex.getNextException(), is(nullValue()));
    }

    @Test
    public void testSetNextException_nullOnPopulatedChain_isNoOp() {
        CatalogException first = new CatalogException("first");
        CatalogException second = new CatalogException("second");
        first.setNextException(second);
        first.setNextException(null);
        assertThat(first.getNextException(), is(second));
        assertThat(second.getNextException(), is(nullValue()));
    }

    @Test
    public void testSetNextException_deepChainAppendsToTail() {
        CatalogException first = new CatalogException("first");
        first.setNextException(new CatalogException("second"));
        first.setNextException(new CatalogException("third"));
        first.setNextException(new CatalogException("fourth"));

        CatalogException node = first;
        int count = 0;
        while (node != null) {
            count++;
            node = node.getNextException();
        }
        assertThat(count, is(4));
    }

    @Test
    public void testExceptionChainMessagesPreserved() {
        CatalogException root = new CatalogException("first");
        root.setNextException(new CatalogException("second"));
        root.setNextException(new CatalogException("third"));

        String[] expected = {"first", "second", "third"};
        int i = 0;
        for (CatalogException rce = root; rce != null; rce = rce.getNextException()) {
            assertThat(rce.getMessage(), is(expected[i++]));
        }
    }

    @Test
    public void testCatalogExceptionExtendsRuntimeException() {
        CatalogException ex = new CatalogException("runtime");

        assertThat(ex instanceof RuntimeException, is(true));
    }

    @Test
    public void testCauseOnlyConstructorUsesCauseToStringAsMessage() {
        IllegalStateException cause = new IllegalStateException("bad state");
        CatalogException ex = new CatalogException(cause);

        assertThat(ex.getMessage(), equalTo(cause.toString()));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testSetNextExceptionAppendsAfterExistingNestedChain() {
        CatalogException first = new CatalogException("first");
        CatalogException second = new CatalogException("second");
        CatalogException third = new CatalogException("third");
        CatalogException fourth = new CatalogException("fourth");

        first.setNextException(second);
        second.setNextException(third);
        first.setNextException(fourth);

        assertThat(first.getNextException(), is(second));
        assertThat(second.getNextException(), is(third));
        assertThat(third.getNextException(), is(fourth));
    }

    @Test
    public void testDeclaresPrivateCatalogExceptionNextField() throws Exception {
        Field field = CatalogException.class.getDeclaredField("m_next_exception");

        assertThat(field.getType(), is(CatalogException.class));
        assertThat(java.lang.reflect.Modifier.isPrivate(field.getModifiers()), is(true));
    }
}
