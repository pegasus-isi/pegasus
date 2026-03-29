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

import org.junit.jupiter.api.Test;

/** Tests for StatCall invocation class. */
public class StatCallTest {

    @Test
    public void testExtendsInvocation() {
        assertTrue(Invocation.class.isAssignableFrom(StatCall.class));
    }

    @Test
    public void testDefaultConstructorNullHandle() {
        StatCall sc = new StatCall();
        assertNull(sc.getHandle());
    }

    @Test
    public void testConstructorWithHandle() {
        StatCall sc = new StatCall("stdin");
        assertEquals("stdin", sc.getHandle());
    }

    @Test
    public void testSetAndGetHandle() {
        StatCall sc = new StatCall();
        sc.setHandle("stdout");
        assertEquals("stdout", sc.getHandle());
    }

    @Test
    public void testSetAndGetLFN() {
        StatCall sc = new StatCall();
        sc.setLFN("output.txt");
        assertEquals("output.txt", sc.getLFN());
    }

    @Test
    public void testSetAndGetError() {
        StatCall sc = new StatCall();
        sc.setError(2);
        assertEquals(2, sc.getError());
    }

    @Test
    public void testSetAndGetStatInfo() {
        StatCall sc = new StatCall();
        StatInfo si = new StatInfo();
        sc.setStatInfo(si);
        assertNotNull(sc.getStatInfo());
    }

    @Test
    public void testSetDataString() {
        StatCall sc = new StatCall();
        sc.setData("some content");
        assertNotNull(sc.getData());
    }
}
