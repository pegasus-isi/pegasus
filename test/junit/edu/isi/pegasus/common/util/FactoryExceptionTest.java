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
package edu.isi.pegasus.common.util;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class FactoryExceptionTest {

    // -----------------------------------------------------------------------
    // Class-level contract
    // -----------------------------------------------------------------------

    @Test
    public void testDefaultNameConstant() {
        assertThat(FactoryException.DEFAULT_NAME, is("Object"));
    }

    @Test
    public void testCanBeCaughtAsRuntimeException() {
        assertThrows(
                RuntimeException.class,
                () -> {
                    throw new FactoryException("thrown as RuntimeException");
                });
    }

    // -----------------------------------------------------------------------
    // Constructors
    // -----------------------------------------------------------------------

    @Test
    public void testConstructor_messageOnly() {
        FactoryException ex = new FactoryException("test error");
        assertThat(ex.getMessage(), is("test error"));
        assertThat(ex.getClassname(), is(FactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructor_messageAndClassname() {
        FactoryException ex = new FactoryException("test error", "MyClass");
        assertThat(ex.getMessage(), is("test error"));
        assertThat(ex.getClassname(), is("MyClass"));
    }

    @Test
    public void testConstructor_messageAndCause() {
        Throwable cause = new RuntimeException("root cause");
        FactoryException ex = new FactoryException("test error", cause);
        assertThat(ex.getMessage(), is("test error"));
        assertThat(ex.getCause(), is(cause));
        assertThat(ex.getClassname(), is(FactoryException.DEFAULT_NAME));
    }

    @Test
    public void testConstructor_messageAndNullCause() {
        FactoryException ex = new FactoryException("test error", (Throwable) null);
        assertThat(ex.getMessage(), is("test error"));
        assertNull(ex.getCause(), "Null cause should be stored as null");
    }

    @Test
    public void testConstructor_messageClassnameCause() {
        Throwable cause = new IllegalArgumentException("bad arg");
        FactoryException ex = new FactoryException("test error", "SomeLoader", cause);
        assertThat(ex.getMessage(), is("test error"));
        assertThat(ex.getClassname(), is("SomeLoader"));
        assertThat(ex.getCause(), is(cause));
    }

    @Test
    public void testConstructor_messageClassnameNullCause() {
        FactoryException ex = new FactoryException("msg", "Cls", null);
        assertThat(ex.getClassname(), is("Cls"));
        assertNull(ex.getCause());
    }

    // -----------------------------------------------------------------------
    // getClassname()
    // -----------------------------------------------------------------------

    @Test
    public void testGetClassname_defaultName() {
        assertThat(new FactoryException("msg").getClassname(), is("Object"));
    }

    @Test
    public void testGetClassname_customName() {
        assertThat(
                new FactoryException("msg", "com.example.Foo").getClassname(),
                is("com.example.Foo"));
    }

    @Test
    public void testGetClassname_customNameWithCause() {
        assertThat(
                new FactoryException("msg", "Loader", new RuntimeException()).getClassname(),
                is("Loader"));
    }

    // -----------------------------------------------------------------------
    // convertException() — zero-arg instance method
    // -----------------------------------------------------------------------

    @Test
    public void testConvertException_containsErrorInfo() {
        FactoryException ex = new FactoryException("something failed", "MyClass");
        String result = ex.convertException();
        assertNotNull(result);
        assertThat(result, containsString("something failed"));
    }

    @Test
    public void testConvertException_outputStartsWithNewlineAndBracket() {
        FactoryException ex = new FactoryException("msg", "Cls");
        String result = ex.convertException();
        assertThat(result, startsWith("\n [1]"));
    }

    @Test
    public void testConvertException_outputContainsAtForStackTrace() {
        FactoryException ex = new FactoryException("msg", "Cls");
        String result = ex.convertException();
        assertThat(result, containsString(" at "));
    }

    @Test
    public void testConvertException_singleEntryNumberedOne() {
        FactoryException ex = new FactoryException("only error");
        String result = ex.convertException();
        assertThat(result, containsString("[1]"));
        assertThat(result, not(containsString("[2]")));
    }

    @Test
    public void testConvertException_withCause_hasTwoEntries() {
        Throwable cause = new RuntimeException("root cause");
        FactoryException ex = new FactoryException("outer error", "Cls", cause);
        String result = ex.convertException();
        assertThat(result, containsString("[1]"));
        assertThat(result, containsString("[2]"));
    }

    @Test
    public void testConvertException_withCause_causeMessagePresent() {
        Throwable cause = new RuntimeException("root cause message");
        FactoryException ex = new FactoryException("outer error", "Cls", cause);
        String result = ex.convertException();
        assertThat(result, containsString("root cause message"));
    }

    // -----------------------------------------------------------------------
    // convertException(int index) — instance method with explicit index
    // -----------------------------------------------------------------------

    @Test
    public void testConvertExceptionWithIndex_zeroSameAsNoArg() {
        FactoryException ex = new FactoryException("msg", "Cls");
        assertThat(ex.convertException(0), is(ex.convertException()));
    }

    @Test
    public void testConvertExceptionWithIndex_firstEntryNumberIsIndexPlusOne() {
        FactoryException ex = new FactoryException("msg", "Cls");
        String result = ex.convertException(4);
        // index=4 → first entry should be [5]
        assertThat(result, containsString("[5]"));
        assertThat(result, not(containsString("[1]")));
    }

    @Test
    public void testConvertExceptionWithIndex_chainedCausesAreNumberedSequentially() {
        Throwable cause = new RuntimeException("root");
        FactoryException ex = new FactoryException("outer", "Cls", cause);
        String result = ex.convertException(10);
        // index=10 → entries [11], [12]
        assertThat(result, containsString("[11]"));
        assertThat(result, containsString("[12]"));
    }

    @Test
    public void testConvertExceptionWithIndex_messageStillPresent() {
        FactoryException ex = new FactoryException("expected message", "Cls");
        String result = ex.convertException(3);
        assertThat(result, containsString("expected message"));
    }

    // -----------------------------------------------------------------------
    // Static convertException(String, Throwable, int)
    // -----------------------------------------------------------------------

    @Test
    public void testStaticConvertException_singleFactoryException() {
        FactoryException fe = new FactoryException("factory load failed", "TargetClass");
        String result = FactoryException.convertException("TargetClass", fe, 0);
        assertThat(result, containsString("[1]"));
        assertThat(result, containsString("factory load failed"));
    }

    @Test
    public void testStaticConvertException_updatesClassnameFromFactoryException() {
        // When a FactoryException is encountered in the chain, its classname replaces
        // the passed-in classname for subsequent entries.
        RuntimeException leaf = new RuntimeException("leaf error");
        FactoryException fe = new FactoryException("factory error", "UpdatedClass", leaf);
        String result = FactoryException.convertException("OriginalClass", fe, 0);
        // [1]: "factory error" (from FactoryException)
        // [2]: leaf — DynamicLoader.convertExceptionToString("UpdatedClass", leaf)
        //   since prev (fe) is a FactoryException → uses DynamicLoader path
        assertThat(result, containsString("factory error"));
        assertThat(result, containsString("[2]"));
    }

    @Test
    public void testStaticConvertException_plainExceptionChain_usesGetMessage() {
        // Pure RuntimeException chain — neither branch matches; uses getMessage()
        RuntimeException root = new RuntimeException("root detail");
        RuntimeException outer = new RuntimeException("outer detail", root);
        String result = FactoryException.convertException("Ctx", outer, 0);
        assertThat(result, containsString("outer detail"));
        assertThat(result, containsString("root detail"));
    }

    @Test
    public void testStaticConvertException_withNonZeroIndex_firstEntryOffset() {
        FactoryException fe = new FactoryException("msg", "Cls");
        String result = FactoryException.convertException("Cls", fe, 5);
        assertThat(result, containsString("[6]"));
        assertThat(result, not(containsString("[1]")));
    }

    @Test
    public void testStaticConvertException_prevIsFactoryException_usesDynamicLoader() {
        // Arrange: outer → FactoryException → RuntimeException (ClassNotFoundException)
        // When the FactoryException is prev and a non-FactoryException follows,
        // DynamicLoader.convertExceptionToString is used with the updated classname.
        ClassNotFoundException leafEx = new ClassNotFoundException("com.missing.Cls");
        FactoryException fe = new FactoryException("load failed", "com.missing.Cls", leafEx);
        String result = FactoryException.convertException("com.missing.Cls", fe, 0);
        // [1]: FactoryException message
        // [2]: DynamicLoader.convertExceptionToString("com.missing.Cls", leafEx)
        //      → "Unable to dynamically load com.missing.Cls"
        assertThat(result, containsString("load failed"));
        assertThat(result, containsString("[2]"));
        // DynamicLoader formats ClassNotFoundException as "Unable to dynamically load ..."
        assertThat(result, containsString("Unable to dynamically load"));
    }

    @Test
    public void testStaticConvertException_outputAlwaysContainsAtForStackTrace() {
        FactoryException fe = new FactoryException("some error", "Cls");
        String result = FactoryException.convertException("Cls", fe, 0);
        assertThat(result, containsString(" at "));
    }

    @Test
    public void testStaticConvertException_deepChain_threeEntries() {
        RuntimeException level3 = new RuntimeException("level 3");
        RuntimeException level2 = new RuntimeException("level 2", level3);
        RuntimeException level1 = new RuntimeException("level 1", level2);
        String result = FactoryException.convertException("Ctx", level1, 0);
        assertThat(result, containsString("[1]"));
        assertThat(result, containsString("[2]"));
        assertThat(result, containsString("[3]"));
        assertThat(result, containsString("level 1"));
        assertThat(result, containsString("level 2"));
        assertThat(result, containsString("level 3"));
    }

    @Test
    public void testStaticConvertException_returnsEmptyStringForNullThrowable() {
        // If e is null, the loop body is never entered and an empty string is returned
        String result = FactoryException.convertException("Cls", null, 0);
        assertThat(result, is(""));
    }
}
