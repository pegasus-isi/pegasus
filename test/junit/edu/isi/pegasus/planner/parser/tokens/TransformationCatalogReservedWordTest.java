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
package edu.isi.pegasus.planner.parser.tokens;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Constructor;
import java.util.Map;
import org.junit.jupiter.api.Test;

/** Tests for {@link TransformationCatalogReservedWord}. */
public class TransformationCatalogReservedWordTest {

    @Test
    public void testSymbolTableContainsTr() {
        Map<String, TransformationCatalogReservedWord> table =
                TransformationCatalogReservedWord.symbolTable();
        assertThat(table.containsKey("tr"), is(true));
    }

    @Test
    public void testSymbolTableContainsSite() {
        assertThat(TransformationCatalogReservedWord.symbolTable().containsKey("site"), is(true));
    }

    @Test
    public void testTrTokenValue() {
        TransformationCatalogReservedWord w =
                TransformationCatalogReservedWord.symbolTable().get("tr");
        assertThat(w.getValue(), is(TransformationCatalogReservedWord.TRANSFORMATION));
    }

    @Test
    public void testSiteTokenValue() {
        TransformationCatalogReservedWord w =
                TransformationCatalogReservedWord.symbolTable().get("site");
        assertThat(w.getValue(), is(TransformationCatalogReservedWord.SITE));
    }

    @Test
    public void testContainerTokenValue() {
        TransformationCatalogReservedWord w =
                TransformationCatalogReservedWord.symbolTable().get("container");
        assertThat(w.getValue(), is(TransformationCatalogReservedWord.CONTAINER));
    }

    @Test
    public void testSymbolTableContainsMountKeyword() {
        assertThat(TransformationCatalogReservedWord.symbolTable().containsKey("mount"), is(true));
    }

    @Test
    public void testConstantValues() {
        assertThat(TransformationCatalogReservedWord.TRANSFORMATION, is(0));
        assertThat(TransformationCatalogReservedWord.SITE, is(1));
        assertThat(TransformationCatalogReservedWord.PROFILE, is(2));
        assertThat(TransformationCatalogReservedWord.PFN, is(3));
    }

    @Test
    public void testIsToken() {
        TransformationCatalogReservedWord w =
                TransformationCatalogReservedWord.symbolTable().get("pfn");
        assertThat(w, instanceOf(Token.class));
    }

    @Test
    public void testSymbolTableReturnsSingletonMapInstance() {
        assertThat(
                TransformationCatalogReservedWord.symbolTable(),
                is(sameInstance(TransformationCatalogReservedWord.symbolTable())));
    }

    @Test
    public void testToStringReturnsKeywordForKnownReservedWord() {
        TransformationCatalogReservedWord w =
                TransformationCatalogReservedWord.symbolTable().get("dockerfile");
        assertThat(w.toString(), is("dockerfile"));
    }

    @Test
    public void testToStringReturnsNullForUnknownReservedWordInstance() throws Exception {
        Constructor<TransformationCatalogReservedWord> constructor =
                TransformationCatalogReservedWord.class.getDeclaredConstructor(int.class);
        constructor.setAccessible(true);
        TransformationCatalogReservedWord w = constructor.newInstance(999);
        assertThat(w.toString(), is(nullValue()));
        assertThat(w.getValue(), is(999));
    }
}
