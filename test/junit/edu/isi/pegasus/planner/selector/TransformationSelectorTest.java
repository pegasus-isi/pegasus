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
package edu.isi.pegasus.planner.selector;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.selector.transformation.Installed;
import edu.isi.pegasus.planner.selector.transformation.Staged;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/** Tests for the TransformationSelector abstract class. */
public class TransformationSelectorTest {

    @Test
    public void testPackageName() {
        assertThat(
                TransformationSelector.PACKAGE_NAME,
                is("edu.isi.pegasus.planner.selector.transformation"));
    }

    @Test
    public void testLoadInstalledSelector() throws Exception {
        TransformationSelector selector = TransformationSelector.loadTXSelector("Installed");
        assertThat(selector, notNullValue());
    }

    @Test
    public void testLoadStagedSelector() throws Exception {
        TransformationSelector selector = TransformationSelector.loadTXSelector("Staged");
        assertThat(selector, notNullValue());
    }

    @Test
    public void testLoadRandomSelector() throws Exception {
        TransformationSelector selector = TransformationSelector.loadTXSelector("Random");
        assertThat(selector, notNullValue());
    }

    @Test
    public void testLoadSubmitSelector() throws Exception {
        TransformationSelector selector = TransformationSelector.loadTXSelector("Submit");
        assertThat(selector, notNullValue());
    }

    @Test
    public void testInstalledSelectorFiltersByType() {
        Installed selector = new Installed();
        List<TransformationCatalogEntry> entries = new ArrayList<>();

        TransformationCatalogEntry installed = new TransformationCatalogEntry("ns", "exe", "1.0");
        installed.setType(TCType.INSTALLED);
        installed.setResourceId("site1");

        TransformationCatalogEntry staged = new TransformationCatalogEntry("ns", "exe", "1.0");
        staged.setType(TCType.STAGEABLE);
        staged.setResourceId("site1");

        entries.add(installed);
        entries.add(staged);

        List result = selector.getTCEntry(entries, "site1");
        assertThat(result, notNullValue());
        assertThat(result.size(), is(1));
    }

    @Test
    public void testStagedSelectorFiltersByType() {
        Staged selector = new Staged();
        List<TransformationCatalogEntry> entries = new ArrayList<>();

        TransformationCatalogEntry installed = new TransformationCatalogEntry("ns", "exe", "1.0");
        installed.setType(TCType.INSTALLED);
        installed.setResourceId("site1");

        TransformationCatalogEntry staged = new TransformationCatalogEntry("ns", "exe", "1.0");
        staged.setType(TCType.STAGEABLE);
        staged.setResourceId("site1");

        entries.add(installed);
        entries.add(staged);

        List result = selector.getTCEntry(entries, "site1");
        assertThat(result, notNullValue());
        assertThat(result.size(), is(1));
    }

    @Test
    public void testLoadInvalidSelectorThrows() {
        assertThrows(
                Exception.class,
                () -> TransformationSelector.loadTXSelector("NonExistentSelector"),
                "Loading invalid selector should throw an exception");
    }

    @Test
    public void testTransformationSelectorIsAbstract() {
        assertThat(Modifier.isAbstract(TransformationSelector.class.getModifiers()), is(true));
    }

    @Test
    public void testGetTCEntryMethodSignature() throws Exception {
        assertThat(
                TransformationSelector.class
                        .getMethod("getTCEntry", List.class, String.class)
                        .getReturnType(),
                is(List.class));
        assertThat(
                Modifier.isAbstract(
                        TransformationSelector.class
                                .getMethod("getTCEntry", List.class, String.class)
                                .getModifiers()),
                is(true));
    }

    @Test
    public void testLoggerFieldExistsAndIsProtected() throws Exception {
        java.lang.reflect.Field field = TransformationSelector.class.getDeclaredField("mLogger");
        assertThat(field.getType(), is(edu.isi.pegasus.common.logging.LogManager.class));
        assertThat(Modifier.isProtected(field.getModifiers()), is(true));
    }
}
