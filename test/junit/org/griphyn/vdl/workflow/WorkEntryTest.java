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
package org.griphyn.vdl.workflow;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;
import org.griphyn.vdl.dbschema.DatabaseSchema;
import org.griphyn.vdl.dbschema.WF;
import org.junit.jupiter.api.Test;

/** @author Rajiv Mayani */
public class WorkEntryTest {

    @Test
    public void workflowHarnessPrintsAllWorkflowEntries() throws Exception {
        WorkEntry first = new WorkEntry(1L, "/tmp/run", "group", "wf-a", "0001");
        WorkEntry second = new WorkEntry(2L, "/tmp/run", "group", "wf-b", "0002");
        java.util.Date now = new java.util.Date();
        first.setCreationTime(now);
        first.setModificationTime(now);
        second.setCreationTime(now);
        second.setModificationTime(now);
        FakeWorkflowSchema schema = new FakeWorkflowSchema(first, second);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        PrintStream original = System.out;
        try {
            System.setOut(new PrintStream(out));
            Map workflows = ((WF) schema).getWorkflows(null);
            for (Object value : workflows.values()) {
                System.out.println(value.toString());
            }
        } finally {
            System.setOut(original);
        }

        String printed = out.toString();
        assertThat(printed, containsString(first.toString()));
        assertThat(printed, containsString(second.toString()));
    }

    private static final class FakeWorkflowSchema extends DatabaseSchema implements WF {
        private final Map<Long, WorkEntry> workflows = new LinkedHashMap<>();

        private FakeWorkflowSchema(WorkEntry... entries) {
            for (WorkEntry entry : entries) {
                workflows.put(entry.getID(), entry);
            }
        }

        @Override
        public WorkEntry getWorkflow(String basedir, String vogroup, String label, String run)
                throws SQLException {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map getWorkflows(java.util.Date mtime) throws SQLException {
            return workflows;
        }
    }
}
