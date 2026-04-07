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
package edu.isi.pegasus.planner.selector.site;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import edu.isi.pegasus.planner.selector.SiteSelector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Group site selector. */
public class GroupTest {

    private Group mSelector;

    @BeforeEach
    public void setUp() {
        mSelector = new Group();
    }

    @Test
    public void testDescription() {
        String desc = mSelector.description();
        assertThat(desc, not(isEmptyOrNullString()));
    }

    @Test
    public void testDescriptionContainsGroup() {
        String desc = mSelector.description().toLowerCase();
        assertThat(desc.toLowerCase(), containsString("group"));
    }

    @Test
    public void testImplementsSiteSelector() {
        assertThat(mSelector, instanceOf(SiteSelector.class));
    }

    @Test
    public void testExtendsAbstract() {
        assertThat(mSelector, instanceOf(Abstract.class));
    }

    @Test
    public void testInstantiationWithDefaultConstructor() {
        Group selector = new Group();
        assertThat(selector, notNullValue());
    }

    @Test
    public void testDescriptionIsNotNull() {
        assertThat(mSelector.description(), notNullValue());
    }

    @Test
    public void testDescriptionExactValue() {
        assertThat(
                mSelector.description(),
                is(
                        "Site selector doing clustering on the basis of key group in pegasus namespace"));
    }

    @Test
    public void testPrivateConstantsAndFieldsExist() throws Exception {
        java.lang.reflect.Field defaultGroup = Group.class.getDeclaredField("mDefaultGroup");
        java.lang.reflect.Field groupMap = Group.class.getDeclaredField("mGroupMap");
        java.lang.reflect.Field selector = Group.class.getDeclaredField("mSelector");

        defaultGroup.setAccessible(true);

        assertThat(defaultGroup.get(null), is("default"));
        assertThat(groupMap.getType(), is(java.util.Map.class));
        assertThat(selector.getType(), is(AbstractPerJob.class));
    }

    @Test
    public void testPrivateInsertMethodExists() throws Exception {
        assertThat(
                Group.class
                        .getDeclaredMethod("insert", edu.isi.pegasus.planner.classes.Job.class)
                        .getReturnType(),
                is(Void.TYPE));
    }
}
