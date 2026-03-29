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

/** Additional Separator.split() tests covering the demo cases from SeparatorTest in src/ */
public class SeparatorTestTest {

    @Test
    public void testSplit_nameWithVersionRange() {
        String[] x = Separator.split("test::me:a,b");
        assertThat(x.length, is(4));
        assertThat(x[0], is("test"));
        assertThat(x[1], is("me"));
        assertThat(x[2], is("a"));
        assertThat(x[3], is("b"));
    }

    @Test
    public void testSplit_minVersionOnly() {
        String[] x = Separator.split("me:a,");
        assertThat(x.length, is(4));
        assertThat(x[1], is("me"));
        assertThat(x[2], is("a"));
        assertNull(x[3]);
    }

    @Test
    public void testSplit_maxVersionOnly_isIllegal() {
        // "me:,b" (no min version, has max) is illegal per Separator spec
        assertThrows(IllegalArgumentException.class, () -> Separator.split("me:,b"));
    }

    @Test
    public void testSplit_illegalTripleColon() {
        assertThrows(IllegalArgumentException.class, () -> Separator.split("illegal:::too"));
    }

    @Test
    public void testSplit_illegalEmptyRange() {
        assertThrows(IllegalArgumentException.class, () -> Separator.split("il::legal:,"));
    }
}
