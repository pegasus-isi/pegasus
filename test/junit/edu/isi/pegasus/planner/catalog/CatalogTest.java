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
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Properties;
import org.junit.jupiter.api.Test;

/** Tests for the Catalog interface constants and structure. */
public class CatalogTest {

    // Minimal concrete implementation for behavioral contract tests
    private static class StubCatalog implements Catalog {
        private boolean mClosed = true;

        @Override
        public boolean connect(Properties props) {
            mClosed = false;
            return true;
        }

        @Override
        public void close() {
            mClosed = true;
        }

        @Override
        public boolean isClosed() {
            return mClosed;
        }
    }

    @Test
    public void testCatalogIsAnInterface() {
        assertThat(Catalog.class.isInterface(), is(true));
    }

    @Test
    public void testDbAllPrefixConstant() {
        assertThat(Catalog.DB_ALL_PREFIX, equalTo("pegasus.catalog.*.db"));
    }

    @Test
    public void testParserDocumentSizePropertyKeyConstant() {
        assertThat(Catalog.PARSER_DOCUMENT_SIZE_PROPERTY_KEY, equalTo("parser.document.size"));
    }

    @Test
    public void testDbAllPrefixIsPublicStaticFinal() throws NoSuchFieldException {
        Field f = Catalog.class.getDeclaredField("DB_ALL_PREFIX");
        int mods = f.getModifiers();
        assertThat(Modifier.isPublic(mods), is(true));
        assertThat(Modifier.isStatic(mods), is(true));
        assertThat(Modifier.isFinal(mods), is(true));
    }

    @Test
    public void testParserDocumentSizePropertyKeyIsPublicStaticFinal() throws NoSuchFieldException {
        Field f = Catalog.class.getDeclaredField("PARSER_DOCUMENT_SIZE_PROPERTY_KEY");
        int mods = f.getModifiers();
        assertThat(Modifier.isPublic(mods), is(true));
        assertThat(Modifier.isStatic(mods), is(true));
        assertThat(Modifier.isFinal(mods), is(true));
    }

    @Test
    public void testCatalogDeclaresMethods() {
        boolean hasConnect = false;
        boolean hasClose = false;
        boolean hasIsClosed = false;
        for (Method m : Catalog.class.getDeclaredMethods()) {
            if (m.getName().equals("connect")) hasConnect = true;
            if (m.getName().equals("close")) hasClose = true;
            if (m.getName().equals("isClosed")) hasIsClosed = true;
        }
        assertThat(hasConnect, is(true));
        assertThat(hasClose, is(true));
        assertThat(hasIsClosed, is(true));
    }

    @Test
    public void testConnectMethodSignature() throws NoSuchMethodException {
        Method connect = Catalog.class.getDeclaredMethod("connect", Properties.class);
        assertThat(connect.getReturnType(), is(boolean.class));
        assertThat(connect.getParameterCount(), is(1));
        assertThat(connect.getParameterTypes()[0], is(Properties.class));
    }

    @Test
    public void testCloseMethodSignature() throws NoSuchMethodException {
        Method close = Catalog.class.getDeclaredMethod("close");
        assertThat(close.getReturnType(), is(void.class));
        assertThat(close.getParameterCount(), is(0));
    }

    @Test
    public void testIsClosedMethodSignature() throws NoSuchMethodException {
        Method isClosed = Catalog.class.getDeclaredMethod("isClosed");
        assertThat(isClosed.getReturnType(), is(boolean.class));
        assertThat(isClosed.getParameterCount(), is(0));
    }

    @Test
    public void testStubCatalog_isClosedBeforeConnect() {
        StubCatalog catalog = new StubCatalog();
        assertThat(catalog.isClosed(), is(true));
    }

    @Test
    public void testStubCatalog_connectReturnsTrueAndOpens() {
        StubCatalog catalog = new StubCatalog();
        boolean connected = catalog.connect(new Properties());
        assertThat(connected, is(true));
        assertThat(catalog.isClosed(), is(false));
    }

    @Test
    public void testStubCatalog_closeTransitionsToClosedState() {
        StubCatalog catalog = new StubCatalog();
        catalog.connect(new Properties());
        assertThat(catalog.isClosed(), is(false));
        catalog.close();
        assertThat(catalog.isClosed(), is(true));
    }

    @Test
    public void testStubCatalog_implementsCatalog() {
        assertThat(new StubCatalog() instanceof Catalog, is(true));
    }

    @Test
    public void testCatalogMethodsArePublicAndAbstract() throws NoSuchMethodException {
        Method connect = Catalog.class.getDeclaredMethod("connect", Properties.class);
        Method close = Catalog.class.getDeclaredMethod("close");
        Method isClosed = Catalog.class.getDeclaredMethod("isClosed");

        for (Method method : new Method[] {connect, close, isClosed}) {
            int mods = method.getModifiers();
            assertThat(Modifier.isPublic(mods), is(true));
            assertThat(Modifier.isAbstract(mods), is(true));
        }
    }

    @Test
    public void testCatalogDeclaresOnlyExpectedMethods() {
        Method[] methods = Catalog.class.getDeclaredMethods();

        assertThat(methods.length, is(3));
    }

    @Test
    public void testCatalogConstantsAreStrings() throws NoSuchFieldException {
        assertThat(Catalog.class.getDeclaredField("DB_ALL_PREFIX").getType(), is(String.class));
        assertThat(
                Catalog.class.getDeclaredField("PARSER_DOCUMENT_SIZE_PROPERTY_KEY").getType(),
                is(String.class));
    }
}
