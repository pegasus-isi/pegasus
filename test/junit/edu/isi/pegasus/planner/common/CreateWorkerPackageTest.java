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
package edu.isi.pegasus.planner.common;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/**
 * Structural tests for CreateWorkerPackage via reflection.
 *
 * @author Rajiv Mayani
 */
public class CreateWorkerPackageTest {

    @Test
    public void testCreateWorkerPackageIsConcreteClass() {
        assertFalse(
                Modifier.isAbstract(CreateWorkerPackage.class.getModifiers()),
                "CreateWorkerPackage should be a concrete class");
    }

    @Test
    public void testCreateWorkerPackageIsNotInterface() {
        assertFalse(
                CreateWorkerPackage.class.isInterface(),
                "CreateWorkerPackage should not be an interface");
    }

    @Test
    public void testHasConstructorWithPegasusBag() throws NoSuchMethodException {
        Constructor<?> c =
                CreateWorkerPackage.class.getConstructor(
                        edu.isi.pegasus.planner.classes.PegasusBag.class);
        assertNotNull(c, "CreateWorkerPackage should have a constructor accepting PegasusBag");
    }

    @Test
    public void testHasCopyMethod() throws NoSuchMethodException {
        Method copyMethod = CreateWorkerPackage.class.getMethod("copy");
        assertNotNull(copyMethod, "CreateWorkerPackage should have a copy() method");
    }

    @Test
    public void testHasCopyMethodWithFileArg() throws NoSuchMethodException {
        Method copyMethod = CreateWorkerPackage.class.getMethod("copy", java.io.File.class);
        assertNotNull(copyMethod, "CreateWorkerPackage should have a copy(File) method");
    }

    @Test
    public void testCopyMethodReturnsFile() throws NoSuchMethodException {
        Method copyMethod = CreateWorkerPackage.class.getMethod("copy");
        assertEquals(
                java.io.File.class,
                copyMethod.getReturnType(),
                "copy() should return a File object");
    }

    @Test
    public void testCopyMethodWithFileArgReturnsFile() throws NoSuchMethodException {
        Method copyMethod = CreateWorkerPackage.class.getMethod("copy", java.io.File.class);
        assertEquals(
                java.io.File.class,
                copyMethod.getReturnType(),
                "copy(File) should return a File object");
    }

    @Test
    public void testCopyMethodIsPublic() throws NoSuchMethodException {
        Method copyMethod = CreateWorkerPackage.class.getMethod("copy");
        assertTrue(Modifier.isPublic(copyMethod.getModifiers()), "copy() method should be public");
    }
}
