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
package edu.isi.pegasus.planner.parser;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.sameInstance;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class SiteCatalogXMLParserTest {

    @Test
    public void testInterfaceShape() {
        assertThat(
                "SiteCatalogXMLParser should be an interface",
                SiteCatalogXMLParser.class.isInterface(),
                is(true));
        assertThat(
                "SiteCatalogXMLParser should not extend other interfaces",
                SiteCatalogXMLParser.class.getInterfaces().length,
                is(0));
    }

    @Test
    public void testGetSiteStoreMethodSignature() throws Exception {
        Method method = SiteCatalogXMLParser.class.getMethod("getSiteStore");

        assertThat(
                "getSiteStore should return SiteStore",
                method.getReturnType(),
                sameInstance((Class<?>) SiteStore.class));
        assertThat(
                "interface methods should be public",
                Modifier.isPublic(method.getModifiers()),
                is(true));
        assertThat(
                "interface methods should be abstract",
                Modifier.isAbstract(method.getModifiers()),
                is(true));
    }

    @Test
    public void testDeclaredMethodCount() {
        assertThat(
                "SiteCatalogXMLParser should declare one method",
                SiteCatalogXMLParser.class.getDeclaredMethods().length,
                is(1));
    }
}
