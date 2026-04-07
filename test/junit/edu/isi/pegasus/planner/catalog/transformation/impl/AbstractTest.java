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
package edu.isi.pegasus.planner.catalog.transformation.impl;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.common.PegasusProperties;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class AbstractTest {

    @Test
    public void testAbstractClassShape() throws Exception {
        assertThat(Modifier.isAbstract(Abstract.class.getModifiers()), is(true));
        assertThat(Abstract.class.getInterfaces()[0], is(TransformationCatalog.class));

        Field loggerField = Abstract.class.getDeclaredField("mLogger");
        Field propsField = Abstract.class.getDeclaredField("mProps");
        Field transientField = Abstract.class.getDeclaredField("mTransient");

        assertThat(loggerField.getType(), is(LogManager.class));
        assertThat(propsField.getType(), is(PegasusProperties.class));
        assertThat(transientField.getType(), is(boolean.class));
    }

    @Test
    public void testModifyForFileURLsInstalledEntryConvertsFileUrlToPath() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry();
        entry.setType(TCType.INSTALLED);
        entry.setPhysicalTransformation("file:///bin/pegasus-plan");

        TransformationCatalogEntry result = Abstract.modifyForFileURLS(entry);

        assertThat(result, sameInstance(entry));
        assertThat(entry.getPhysicalTransformation(), equalTo("/bin/pegasus-plan"));
    }

    @Test
    public void testModifyForFileURLsStageableEntryConvertsAbsolutePathToFileUrl() {
        TransformationCatalogEntry entry = new TransformationCatalogEntry();
        entry.setType(TCType.STAGEABLE);
        entry.setPhysicalTransformation("/opt/pegasus/bin/tool");

        TransformationCatalogEntry result = Abstract.modifyForFileURLS(entry);

        assertThat(result, sameInstance(entry));
        assertThat(entry.getPhysicalTransformation(), equalTo("file:///opt/pegasus/bin/tool"));
    }

    @Test
    public void testModifyForFileURLsStringOverloadHandlesInstalledAndStageablePfns() {
        assertThat(
                Abstract.modifyForFileURLS("file:///usr/bin/kickstart", TCType.INSTALLED.name()),
                is("/usr/bin/kickstart"));
        assertThat(
                Abstract.modifyForFileURLS("/srv/stageable/tool", TCType.STAGEABLE.name()),
                is("file:///srv/stageable/tool"));
    }

    @Test
    public void testModifyForFileURLsReturnsInputForNullOrUnchangedValues() {
        assertThat(
                Abstract.modifyForFileURLS((String) null, TCType.INSTALLED.name()),
                is(nullValue()));

        TransformationCatalogEntry entry = new TransformationCatalogEntry();
        entry.setType(TCType.INSTALLED);
        entry.setPhysicalTransformation("gsiftp://example/path/tool");

        assertThat(Abstract.modifyForFileURLS(entry), is(sameInstance(entry)));
        assertThat(entry.getPhysicalTransformation(), equalTo("gsiftp://example/path/tool"));
    }
}
