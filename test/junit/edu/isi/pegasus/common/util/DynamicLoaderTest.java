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

import java.lang.reflect.InvocationTargetException;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class DynamicLoaderTest {

    // -----------------------------------------------------------------------
    // Constructor
    // -----------------------------------------------------------------------

    @Test
    public void testConstructor_setsClassName() {
        DynamicLoader dl = new DynamicLoader("java.lang.String");
        assertThat(dl.getClassName(), is("java.lang.String"));
    }

    @Test
    public void testConstructor_nullThrows() {
        assertThrows(NullPointerException.class, () -> new DynamicLoader(null));
    }

    // -----------------------------------------------------------------------
    // setClassName / getClassName
    // -----------------------------------------------------------------------

    @Test
    public void testSetClassName() {
        DynamicLoader dl = new DynamicLoader("java.lang.String");
        dl.setClassName("java.lang.Integer");
        assertThat(dl.getClassName(), is("java.lang.Integer"));
    }

    @Test
    public void testSetClassName_nullThrows() {
        DynamicLoader dl = new DynamicLoader("java.lang.String");
        assertThrows(NullPointerException.class, () -> dl.setClassName(null));
    }

    @Test
    public void testSetClassName_preservedAfterMultipleUpdates() {
        DynamicLoader dl = new DynamicLoader("java.lang.String");
        dl.setClassName("java.lang.Long");
        dl.setClassName("java.util.ArrayList");
        assertThat(dl.getClassName(), is("java.util.ArrayList"));
    }

    // -----------------------------------------------------------------------
    // instantiate(Object[]) — infer classes from argument types
    // -----------------------------------------------------------------------

    @Test
    public void testInstantiate_noArgConstructor() throws Exception {
        DynamicLoader dl = new DynamicLoader("java.util.ArrayList");
        Object result = dl.instantiate(new Object[0]);
        assertThat(result, instanceOf(java.util.ArrayList.class));
    }

    @Test
    public void testInstantiate_stringWithArg() throws Exception {
        DynamicLoader dl = new DynamicLoader("java.lang.Integer");
        Object result = dl.instantiate(new Object[] {"42"});
        assertThat(result, instanceOf(Integer.class));
        assertThat((Integer) result, is(42));
    }

    @Test
    public void testInstantiate_returnedInstanceIsCorrectValue() throws Exception {
        DynamicLoader dl = new DynamicLoader("java.lang.Integer");
        Object result = dl.instantiate(new Object[] {"99"});
        assertThat(result, is(99));
    }

    @Test
    public void testInstantiate_stringNoArgConstructor() throws Exception {
        DynamicLoader dl = new DynamicLoader("java.lang.String");
        Object result = dl.instantiate(new Object[0]);
        assertThat(result, instanceOf(String.class));
        assertThat(result, is(""));
    }

    @Test
    public void testInstantiate_classNotFound() {
        DynamicLoader dl = new DynamicLoader("com.example.NonExistent");
        assertThrows(ClassNotFoundException.class, () -> dl.instantiate(new Object[0]));
    }

    @Test
    public void testInstantiate_noSuchMethod_wrongArgType() {
        // Integer has no constructor taking an Integer argument
        DynamicLoader dl = new DynamicLoader("java.lang.Integer");
        assertThrows(
                NoSuchMethodException.class,
                () -> dl.instantiate(new Object[] {Integer.valueOf(42)}),
                "Passing wrong argument type should throw NoSuchMethodException");
    }

    // -----------------------------------------------------------------------
    // instantiate(Class[], Object[]) — explicit class array
    // -----------------------------------------------------------------------

    @Test
    public void testInstantiateWithClasses_stringConstructor() throws Exception {
        DynamicLoader dl = new DynamicLoader("java.lang.Integer");
        Object result = dl.instantiate(new Class[] {String.class}, new Object[] {"123"});
        assertThat(result, instanceOf(Integer.class));
        assertThat(result, is(123));
    }

    @Test
    public void testInstantiateWithClasses_noArgConstructor() throws Exception {
        DynamicLoader dl = new DynamicLoader("java.util.ArrayList");
        Object result = dl.instantiate(new Class[0], new Object[0]);
        assertThat(result, instanceOf(java.util.ArrayList.class));
    }

    @Test
    public void testInstantiateWithClasses_mismatchedSizesThrows() {
        DynamicLoader dl = new DynamicLoader("java.lang.Integer");
        assertThrows(
                IllegalArgumentException.class,
                () -> dl.instantiate(new Class[] {String.class, String.class}, new Object[] {"42"}),
                "Mismatched class/argument array sizes must throw IllegalArgumentException");
    }

    @Test
    public void testInstantiateWithClasses_emptyMismatchThrows() {
        DynamicLoader dl = new DynamicLoader("java.lang.Integer");
        assertThrows(
                IllegalArgumentException.class,
                () -> dl.instantiate(new Class[] {String.class}, new Object[0]),
                "One class, zero arguments must throw IllegalArgumentException");
    }

    @Test
    public void testInstantiateWithClasses_classNotFound() {
        DynamicLoader dl = new DynamicLoader("com.example.NonExistent");
        assertThrows(
                ClassNotFoundException.class, () -> dl.instantiate(new Class[0], new Object[0]));
    }

    @Test
    public void testInstantiateWithClasses_noSuchMethod() {
        // Integer has no constructor taking a Long
        DynamicLoader dl = new DynamicLoader("java.lang.Integer");
        assertThrows(
                NoSuchMethodException.class,
                () -> dl.instantiate(new Class[] {Long.class}, new Object[] {42L}));
    }

    // -----------------------------------------------------------------------
    // static_method
    // -----------------------------------------------------------------------

    @Test
    public void testStaticMethod_integerValueOf() throws Exception {
        DynamicLoader dl = new DynamicLoader("java.lang.Integer");
        Object result = dl.static_method("valueOf", new Object[] {"100"});
        assertThat(result, instanceOf(Integer.class));
        assertThat((Integer) result, is(100));
    }

    @Test
    public void testStaticMethod_returnsCorrectValue() throws Exception {
        DynamicLoader dl = new DynamicLoader("java.lang.Integer");
        Object result = dl.static_method("valueOf", new Object[] {"255"});
        assertThat(result, is(255));
    }

    @Test
    public void testStaticMethod_noSuchMethod() {
        DynamicLoader dl = new DynamicLoader("java.lang.Integer");
        assertThrows(
                NoSuchMethodException.class,
                () -> dl.static_method("nonExistentMethod", new Object[] {"x"}),
                "Calling a non-existent static method should throw NoSuchMethodException");
    }

    @Test
    public void testStaticMethod_classNotFound() {
        DynamicLoader dl = new DynamicLoader("com.example.NoSuchClass");
        assertThrows(
                ClassNotFoundException.class, () -> dl.static_method("someMethod", new Object[0]));
    }

    @Test
    public void testStaticMethod_invocationTargetException() {
        // Integer.valueOf(String) throws NumberFormatException for bad input,
        // which gets wrapped in InvocationTargetException
        DynamicLoader dl = new DynamicLoader("java.lang.Integer");
        assertThrows(
                InvocationTargetException.class,
                () -> dl.static_method("valueOf", new Object[] {"not-a-number"}),
                "Static method that throws should wrap the exception in InvocationTargetException");
    }

    // -----------------------------------------------------------------------
    // convertExceptionToString — every exception branch
    // -----------------------------------------------------------------------

    @Test
    public void testConvertExceptionToString_classNotFound_containsClassName() {
        ClassNotFoundException ex = new ClassNotFoundException("com.missing.Foo");
        String msg = DynamicLoader.convertExceptionToString("com.missing.Foo", ex);
        assertThat(msg, containsString("com.missing.Foo"));
    }

    @Test
    public void testConvertExceptionToString_classNotFound_startsWithUnableToLoad() {
        ClassNotFoundException ex = new ClassNotFoundException("com.missing.Foo");
        String msg = DynamicLoader.convertExceptionToString("com.missing.Foo", ex);
        assertThat(msg, startsWith("Unable to dynamically load"));
    }

    @Test
    public void testConvertExceptionToString_noSuchMethod_containsMethodName() {
        NoSuchMethodException ex = new NoSuchMethodException("badMethod");
        String msg = DynamicLoader.convertExceptionToString("SomeClass", ex);
        assertThat(msg, containsString("badMethod"));
    }

    @Test
    public void testConvertExceptionToString_noSuchMethod_containsInvokePhrase() {
        NoSuchMethodException ex = new NoSuchMethodException("badMethod");
        String msg = DynamicLoader.convertExceptionToString("SomeClass", ex);
        assertThat(msg, containsString("invoke"));
    }

    @Test
    public void testConvertExceptionToString_instantiationException_containsClassName() {
        InstantiationException ex = new InstantiationException("abstract");
        String msg = DynamicLoader.convertExceptionToString("AbstractClass", ex);
        assertThat(msg, containsString("AbstractClass"));
    }

    @Test
    public void testConvertExceptionToString_instantiationException_mentionsAbstractOrInterface() {
        InstantiationException ex = new InstantiationException("abstract");
        String msg = DynamicLoader.convertExceptionToString("AbstractClass", ex);
        assertThat(msg, containsString("abstract"));
    }

    @Test
    public void testConvertExceptionToString_illegalAccess_containsClassName() {
        IllegalAccessException ex = new IllegalAccessException("no access");
        String msg = DynamicLoader.convertExceptionToString("PrivateClass", ex);
        assertThat(msg, containsString("PrivateClass"));
    }

    @Test
    public void testConvertExceptionToString_illegalAccess_mentionsConstructor() {
        IllegalAccessException ex = new IllegalAccessException("no access");
        String msg = DynamicLoader.convertExceptionToString("PrivateClass", ex);
        assertThat(msg, containsString("constructor"));
    }

    @Test
    public void testConvertExceptionToString_invocationTarget_containsClassName() {
        InvocationTargetException ex = new InvocationTargetException(new RuntimeException("inner"));
        String msg = DynamicLoader.convertExceptionToString("MyClass", ex);
        assertThat(msg, containsString("MyClass"));
    }

    @Test
    public void testConvertExceptionToString_invocationTarget_mentionsDuringConstruction() {
        InvocationTargetException ex = new InvocationTargetException(new RuntimeException("inner"));
        String msg = DynamicLoader.convertExceptionToString("MyClass", ex);
        assertThat(msg, containsString("during construction"));
    }

    @Test
    public void testConvertExceptionToString_illegalArgument_containsClassName() {
        IllegalArgumentException ex = new IllegalArgumentException("bad args");
        String msg = DynamicLoader.convertExceptionToString("MyClass", ex);
        assertThat(msg, containsString("MyClass"));
    }

    @Test
    public void testConvertExceptionToString_illegalArgument_mentionsMethodInvocation() {
        IllegalArgumentException ex = new IllegalArgumentException("bad args");
        String msg = DynamicLoader.convertExceptionToString("MyClass", ex);
        assertThat(msg, containsString("method invocation"));
    }

    @Test
    public void testConvertExceptionToString_nullPointer_mentionsInvalidInitializer() {
        NullPointerException ex = new NullPointerException("null method");
        String msg = DynamicLoader.convertExceptionToString("MyClass", ex);
        assertThat(msg, containsString("Invalid static initializer method name"));
    }

    @Test
    public void testConvertExceptionToString_nullPointer_containsClassName() {
        NullPointerException ex = new NullPointerException("null method");
        String msg = DynamicLoader.convertExceptionToString("MyClass", ex);
        assertThat(msg, containsString("MyClass"));
    }

    @Test
    public void testConvertExceptionToString_securityException_mentionsProhibitedAccess() {
        SecurityException ex = new SecurityException("denied");
        String msg = DynamicLoader.convertExceptionToString("SecureClass", ex);
        assertThat(msg, containsString("Prohibited access"));
    }

    @Test
    public void testConvertExceptionToString_securityException_containsClassName() {
        SecurityException ex = new SecurityException("denied");
        String msg = DynamicLoader.convertExceptionToString("SecureClass", ex);
        assertThat(msg, containsString("SecureClass"));
    }

    @Test
    public void testConvertExceptionToString_unknownException_containsClassName() {
        // An exception type not matched by any branch falls through to the else clause
        RuntimeException ex = new RuntimeException("unexpected");
        String msg = DynamicLoader.convertExceptionToString("SomeClass", ex);
        assertThat(msg, containsString("SomeClass"));
    }

    @Test
    public void testConvertExceptionToString_unknownException_containsCaughtKeyword() {
        RuntimeException ex = new RuntimeException("unexpected");
        String msg = DynamicLoader.convertExceptionToString("SomeClass", ex);
        assertThat(msg, containsString("caught"));
    }

    @Test
    public void testConvertExceptionToString_unknownException_containsExceptionMessage() {
        RuntimeException ex = new RuntimeException("unexpected-detail");
        String msg = DynamicLoader.convertExceptionToString("SomeClass", ex);
        assertThat(msg, containsString("unexpected-detail"));
    }

    @Test
    public void testConvertExceptionToString_unknownException_containsExceptionClassName() {
        RuntimeException ex = new RuntimeException("msg");
        String msg = DynamicLoader.convertExceptionToString("SomeClass", ex);
        assertThat(msg, containsString("RuntimeException"));
    }

    // -----------------------------------------------------------------------
    // convertException(String, Exception) — static overload, cause chain
    // -----------------------------------------------------------------------

    @Test
    public void testConvertException_static_noCause() {
        ClassNotFoundException ex = new ClassNotFoundException("com.missing.Bar");
        String msg = DynamicLoader.convertException("com.missing.Bar", ex);
        assertThat(msg, containsString("com.missing.Bar"));
        // No cause — no "[1]:" suffix
        assertThat(msg, not(containsString("[1]")));
    }

    @Test
    public void testConvertException_static_singleCause() {
        RuntimeException cause = new RuntimeException("root cause");
        ClassNotFoundException ex = new ClassNotFoundException("com.missing.Baz", cause);
        String msg = DynamicLoader.convertException("com.missing.Baz", ex);
        assertThat(msg, containsString("[1]"));
        assertThat(msg, containsString("root cause"));
    }

    @Test
    public void testConvertException_static_chainedCauses() {
        RuntimeException rootCause = new RuntimeException("root");
        RuntimeException midCause = new RuntimeException("middle", rootCause);
        ClassNotFoundException ex = new ClassNotFoundException("SomeClass", midCause);
        String msg = DynamicLoader.convertException("SomeClass", ex);
        assertThat(msg, containsString("[1]"));
        assertThat(msg, containsString("[2]"));
        assertThat(msg, containsString("middle"));
        assertThat(msg, containsString("root"));
    }

    // -----------------------------------------------------------------------
    // convertException(Exception) — instance overload delegates to static
    // -----------------------------------------------------------------------

    @Test
    public void testConvertException_instanceMethod_usesStoredClassName() {
        DynamicLoader dl = new DynamicLoader("com.example.Missing");
        ClassNotFoundException ex = new ClassNotFoundException("com.example.Missing");
        String msg = dl.convertException(ex);
        assertThat(msg, containsString("com.example.Missing"));
    }

    @Test
    public void testConvertException_instanceMethod_afterSetClassName() {
        DynamicLoader dl = new DynamicLoader("com.example.Old");
        dl.setClassName("com.example.New");
        ClassNotFoundException ex = new ClassNotFoundException("com.example.New");
        String msg = dl.convertException(ex);
        // Should use the updated class name, not the original one
        assertThat(msg, containsString("com.example.New"));
    }

    @Test
    public void testConvertException_instanceMethod_withCause() {
        DynamicLoader dl = new DynamicLoader("com.example.Cls");
        RuntimeException cause = new RuntimeException("inner detail");
        ClassNotFoundException ex = new ClassNotFoundException("com.example.Cls", cause);
        String msg = dl.convertException(ex);
        assertThat(msg, containsString("[1]"));
        assertThat(msg, containsString("inner detail"));
    }

    @Test
    public void testInstantiateWithExplicitInterfaceTypeSucceeds() throws Exception {
        DynamicLoader dl = new DynamicLoader("java.util.ArrayList");
        java.util.ArrayList<String> source = new java.util.ArrayList<String>();
        source.add("a");
        source.add("b");

        Object result =
                dl.instantiate(new Class[] {java.util.Collection.class}, new Object[] {source});

        assertThat(result, instanceOf(java.util.ArrayList.class));
        assertThat(((java.util.ArrayList<?>) result).size(), is(2));
    }

    @Test
    public void testInstantiateWithInferredImplementationTypeCanMissInterfaceConstructor() {
        DynamicLoader dl = new DynamicLoader("java.util.ArrayList");
        java.util.ArrayList<String> source = new java.util.ArrayList<String>();
        assertThrows(NoSuchMethodException.class, () -> dl.instantiate(new Object[] {source}));
    }

    @Test
    public void testStaticMethodWithNoArguments() throws Exception {
        DynamicLoader dl = new DynamicLoader("java.lang.System");
        Object result = dl.static_method("lineSeparator", new Object[0]);
        assertThat(result, is(System.lineSeparator()));
    }

    @Test
    public void testConvertException_staticInvocationTargetAppendsCauseChain() {
        InvocationTargetException ex =
                new InvocationTargetException(new IllegalStateException("inner-cause"));
        String msg = DynamicLoader.convertException("SomeClass", ex);
        assertThat(msg, containsString("[1]"));
        assertThat(msg, containsString("inner-cause"));
    }
}
