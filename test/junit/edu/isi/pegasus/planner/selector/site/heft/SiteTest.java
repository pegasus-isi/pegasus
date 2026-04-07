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
package edu.isi.pegasus.planner.selector.site.heft;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

/** Tests for the Site class in the HEFT package. */
public class SiteTest {

    @Test
    public void testSiteInstantiationWithName() {
        Site site = new Site("my-site");
        assertThat(site, notNullValue());
    }

    @Test
    public void testSiteInstantiationWithNameAndProcessors() {
        Site site = new Site("my-site", 4);
        assertThat(site, notNullValue());
    }

    @Test
    public void testSiteClassExists() {
        assertThat(Site.class, notNullValue());
    }

    @Test
    public void testGetNameAndAvailableProcessors() {
        Site site = new Site("submit", 4);

        assertThat(site.getName(), is("submit"));
        assertThat(site.getAvailableProcessors(), is(4));
    }

    @Test
    public void testGetAvailableTimeUsesUnusedProcessorAtRequestedStart() throws Exception {
        Site site = new Site("compute", 2);

        assertThat(site.getAvailableTime(5L), is(5L));

        assertThat(((List) ReflectionTestUtils.getField(site, "mProcessors")).size(), is(1));
    }

    @Test
    public void testScheduleJobRequiresTentativeSchedulingFirst() {
        Site site = new Site("compute", 1);

        RuntimeException exception =
                assertThrows(RuntimeException.class, () -> site.scheduleJob(0L, 10L));
        assertThat(exception.getMessage(), containsString("tentatively scheduled first"));
    }

    @Test
    public void testScheduleJobResetsCurrentProcessorAndUpdatesAvailability() throws Exception {
        Site site = new Site("compute", 1);

        assertThat(site.getAvailableTime(3L), is(3L));
        site.scheduleJob(3L, 9L);
        assertThat(site.getAvailableTime(4L), is(9L));

        assertThat((Integer) ReflectionTestUtils.getField(site, "mCurrentProcessorIndex"), is(0));
    }
}
