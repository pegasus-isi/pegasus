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
package edu.isi.pegasus.planner.invocation;

import static org.junit.jupiter.api.Assertions.*;

import java.io.Serializable;
import org.junit.jupiter.api.Test;

/** Tests for InvocationRecord class. */
public class InvocationRecordTest {

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(InvocationRecord.class));
    }

    @Test
    public void testImplementsSerializable() {
        assertTrue(Serializable.class.isAssignableFrom(InvocationRecord.class));
    }

    @Test
    public void testSchemaNamespace() {
        assertEquals(
                "https://pegasus.isi.edu/schema/invocation", InvocationRecord.SCHEMA_NAMESPACE);
    }

    @Test
    public void testSchemaLocation() {
        assertTrue(InvocationRecord.SCHEMA_LOCATION.endsWith(".xsd"));
    }

    @Test
    public void testDefaultConstructorCreates() {
        InvocationRecord ir = new InvocationRecord();
        assertNotNull(ir);
    }

    @Test
    public void testSetAndGetTransformation() {
        InvocationRecord ir = new InvocationRecord();
        ir.setTransformation("pegasus::findrange");
        assertEquals("pegasus::findrange", ir.getTransformation());
    }

    @Test
    public void testSetAndGetUID() {
        InvocationRecord ir = new InvocationRecord();
        ir.setUID(1001);
        assertEquals(1001, ir.getUID());
    }

    @Test
    public void testSetAndGetUser() {
        InvocationRecord ir = new InvocationRecord();
        ir.setUser("testuser");
        assertEquals("testuser", ir.getUser());
    }
}
