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
package edu.isi.pegasus.planner.catalog.site.classes;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

/**
 * Tests for FileSystemType via InternalMountPoint (concrete subclass).
 *
 * @author Rajiv Mayani
 */
public class FileSystemTypeTest {

    @Test
    public void testDefaultConstructorEmptyStrings() {
        InternalMountPoint imp = new InternalMountPoint();
        assertThat(imp.getMountPoint(), is(""));
        assertThat(imp.getTotalSize(), is(""));
        assertThat(imp.getFreeSize(), is(""));
    }

    @Test
    public void testOverloadedConstructorWithAllParameters() {
        InternalMountPoint imp = new InternalMountPoint("/data", "100GB", "50GB");
        assertThat(imp.getMountPoint(), is("/data"));
        assertThat(imp.getTotalSize(), is("100GB"));
        assertThat(imp.getFreeSize(), is("50GB"));
    }

    @Test
    public void testSetAndGetMountPoint() {
        InternalMountPoint imp = new InternalMountPoint();
        imp.setMountPoint("/scratch");
        assertThat(imp.getMountPoint(), is("/scratch"));
    }

    @Test
    public void testSetAndGetTotalSize() {
        InternalMountPoint imp = new InternalMountPoint();
        imp.setTotalSize("200GB");
        assertThat(imp.getTotalSize(), is("200GB"));
    }

    @Test
    public void testSetAndGetFreeSize() {
        InternalMountPoint imp = new InternalMountPoint();
        imp.setFreeSize("75GB");
        assertThat(imp.getFreeSize(), is("75GB"));
    }

    @Test
    public void testCloneProducesEqualButDistinctInstance() {
        InternalMountPoint imp = new InternalMountPoint("/work", "512GB", "256GB");
        InternalMountPoint cloned = (InternalMountPoint) imp.clone();
        assertNotSame(imp, cloned);
        assertThat(cloned.getMountPoint(), is(imp.getMountPoint()));
        assertThat(cloned.getTotalSize(), is(imp.getTotalSize()));
        assertThat(cloned.getFreeSize(), is(imp.getFreeSize()));
    }

    @Test
    public void testCloneIsIndependentOfOriginalMutations() {
        InternalMountPoint original = new InternalMountPoint("/work", "512GB", "256GB");
        InternalMountPoint cloned = (InternalMountPoint) original.clone();

        original.setMountPoint("/changed");
        original.setTotalSize("1024GB");
        original.setFreeSize("128GB");

        assertThat(cloned.getMountPoint(), is("/work"));
        assertThat(cloned.getTotalSize(), is("512GB"));
        assertThat(cloned.getFreeSize(), is("256GB"));
    }

    @Test
    public void testSettersOverrideConstructorValues() {
        InternalMountPoint imp = new InternalMountPoint("/data", "100GB", "50GB");

        imp.setMountPoint("/scratch");
        imp.setTotalSize("200GB");
        imp.setFreeSize("75GB");

        assertThat(imp.getMountPoint(), is("/scratch"));
        assertThat(imp.getTotalSize(), is("200GB"));
        assertThat(imp.getFreeSize(), is("75GB"));
    }
}
