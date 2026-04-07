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
package edu.isi.pegasus.common.logging;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import org.junit.jupiter.api.Test;

/** Tests for LoggingKeys constants. */
public class LoggingKeysTest {

    // --- entity ID keys ---

    @Test
    public void testEntityIdConstants() {
        assertThat(LoggingKeys.EVENT_ID_KEY, is("eventId"));
        assertThat(LoggingKeys.PORTFOLIO_ID, is("portfolio.id"));
        assertThat(LoggingKeys.REQUEST_ID, is("request.id"));
        assertThat(LoggingKeys.DAX_ID, is("dax.id"));
        assertThat(LoggingKeys.DAG_ID, is("dag.id"));
        assertThat(LoggingKeys.JOB_ID, is("job.id"));
        assertThat(LoggingKeys.JOB_NUMBER, is("job.id"));
        assertThat(LoggingKeys.SEED_ID, is("seed.id"));
        assertThat(LoggingKeys.TEMPLATE_ID, is("template.id"));
        assertThat(LoggingKeys.SEED_NAME, is("seed.name"));
    }

    @Test
    public void testJobNumberAndJobIdAreAliased() {
        assertThat(LoggingKeys.JOB_NUMBER, is(LoggingKeys.JOB_ID));
    }

    // --- pegasus event keys ---

    @Test
    public void testPegasusPlannerEventConstants() {
        assertThat(LoggingKeys.EVENT_PEGASUS_PLAN, is("event.pegasus.plan"));
        assertThat(LoggingKeys.EVENT_PEGASUS_REFINEMENT, is("event.pegasus.refinement"));
        assertThat(LoggingKeys.EVENT_PEGASUS_REDUCE, is("event.pegasus.reduce"));
        assertThat(LoggingKeys.EVENT_PEGASUS_SITESELECTION, is("event.pegasus.siteselection"));
        assertThat(LoggingKeys.EVENT_PEGASUS_CLUSTER, is("event.pegasus.cluster"));
        assertThat(LoggingKeys.EVENT_PEGASUS_PARTITION, is("event.pegasus.partition"));
        assertThat(LoggingKeys.EVENT_PEGASUS_CODE_GENERATION, is("event.pegasus.code.generation"));
        assertThat(
                LoggingKeys.EVENT_PEGASUS_ADD_TRANSFER_NODES,
                is("event.pegasus.generate.transfer-nodes"));
        assertThat(
                LoggingKeys.EVENT_PEGASUS_GENERATE_CLEANUP,
                is("event.pegasus.generate.cleanup-nodes"));
        assertThat(
                LoggingKeys.EVENT_PEGASUS_GENERATE_WORKDIR,
                is("event.pegasus.generate.workdir-nodes"));
    }

    @Test
    public void testPegasusParsingEventConstants() {
        assertThat(LoggingKeys.EVENT_PEGASUS_PARSE_DAX, is("event.pegasus.parse.dax"));
        assertThat(LoggingKeys.EVENT_PEGASUS_PARSE_PDAX, is("event.pegasus.parse.pdax"));
        assertThat(
                LoggingKeys.EVENT_PEGASUS_PARSE_SITE_CATALOG,
                is("event.pegasus.parse.site-catalog"));
    }

    // --- time, system, and job metadata keys ---

    @Test
    public void testTimeConstants() {
        assertThat(LoggingKeys.TIME_START, is("time.start"));
        assertThat(LoggingKeys.TIME_END, is("time.end"));
    }

    @Test
    public void testSystemConstants() {
        assertThat(LoggingKeys.SYSTEM_HOSTNAME, is("system.hostname"));
        assertThat(LoggingKeys.SYSTEM_HOSTADDR, is("system.hostaddr"));
        assertThat(LoggingKeys.SYSTEM_OS, is("system.os"));
        assertThat(LoggingKeys.SYSTEM_ARCHITECTURE, is("system.architecture"));
        assertThat(LoggingKeys.SYSTEM_NODENAME, is("system.nodename"));
        assertThat(LoggingKeys.SYSTEM_NUMBEROFPROCESSORS, is("system.numberOfProcessors"));
    }

    @Test
    public void testQueryConstants() {
        assertThat(LoggingKeys.QUERY_NUMBER, is("query.number"));
        assertThat(LoggingKeys.QUERY_NAME, is("query.name"));
        assertThat(LoggingKeys.QUERY_ID, is("query.id"));
        assertThat(LoggingKeys.QUERY_INPUT, is("query.input"));
        assertThat(LoggingKeys.QUERY_OUTPUT, is("query.output"));
    }

    // --- sweep: all constants non-null and non-empty ---

    @Test
    public void testAllConstantsAreNonEmpty() throws Exception {
        for (Field f : LoggingKeys.class.getDeclaredFields()) {
            if (f.getType() == String.class && Modifier.isStatic(f.getModifiers())) {
                String value = (String) f.get(null);
                assertThat(value, is(notNullValue()));
                assertThat(value, is(not(emptyString())));
            }
        }
    }

    @Test
    public void testAdditionalEventConstants() {
        assertThat(LoggingKeys.EVENT_QUERY_PROCESSCATALOG, is("event.query.processcatalog"));
        assertThat(LoggingKeys.EVENT_QUERY_DATACATALOG, is("event.query.datacatalog"));
        assertThat(LoggingKeys.EVENT_ENSEMBLE_RANKING, is("event.ensemble.ranking"));
        assertThat(LoggingKeys.EVENT_ENSEMBLE_PLANNING, is("event.ensemble.planning"));
        assertThat(LoggingKeys.EVENT_ENSEMBLE_WG, is("event.ensemble.wings"));
        assertThat(LoggingKeys.EVENT_ENSEMBLE_EXECUTE, is("event.ensemble.workflow.execute"));
        assertThat(LoggingKeys.EVENT_PEGASUS_RANKING, is("event.ranking"));
        assertThat(LoggingKeys.EVENT_PEGASUS_RANKING_RETRIEVE_DAX, is("event.ranking.retrive.dax"));
        assertThat(LoggingKeys.EVENT_WORKFLOW_JOB_STATUS, is("event.workflow.job.status"));
    }

    @Test
    public void testAdditionalMetadataConstants() {
        assertThat(LoggingKeys.QUERY_ARGUMENTS, is("query.arguments"));
        assertThat(LoggingKeys.QUERY_RESPONSE, is("query.response"));
        assertThat(LoggingKeys.FILE_OUTPUT_NAME, is("file.output.name"));
        assertThat(LoggingKeys.FILE_OUTPUT_CONTENTS, is("file.output.contents"));
        assertThat(LoggingKeys.FILE_PRIORTY, is("file.priorty"));
        assertThat(LoggingKeys.FILE_TYPE, is("file.type"));
        assertThat(LoggingKeys.JOB_EXITTCODE, is("job.exittcode"));
        assertThat(LoggingKeys.JOB_ARGUMENTS, is("job.arguments"));
        assertThat(LoggingKeys.JOB_ENVIRONMENTVARIABLE, is("job.environmentVariable"));
        assertThat(LoggingKeys.JOB_RESOURCE_INFORMATION, is("job.resource.information"));
        assertThat(LoggingKeys.PERFMETRIC_CPU_UTILIZATION, is("perfmetric.cpu.utilization"));
        assertThat(LoggingKeys.PERFMETRIC_MEMORY_UTILIZATION, is("perfmetric.memory.utilization"));
        assertThat(LoggingKeys.PERFMETRIC_NETWORK_BANDWIDTH, is("perfmetric.network.bandwidth"));
        assertThat(LoggingKeys.PERFMETRIC_TIME_DURATION, is("perfmetric.time.duration"));
    }

    @Test
    public void testLoggingKeysDeclaresOnlyStringConstants() throws Exception {
        for (Field f : LoggingKeys.class.getDeclaredFields()) {
            assertThat(f.getType(), is((Object) String.class));
            assertThat(Modifier.isPublic(f.getModifiers()), is(true));
            assertThat(Modifier.isStatic(f.getModifiers()), is(true));
            assertThat(Modifier.isFinal(f.getModifiers()), is(true));
        }
    }

    @Test
    public void testLoggingKeysDeclaresNoMethods() {
        assertThat(LoggingKeys.class.getDeclaredMethods(), emptyArray());
    }
}
