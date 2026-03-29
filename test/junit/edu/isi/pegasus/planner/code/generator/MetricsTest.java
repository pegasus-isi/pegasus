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
package edu.isi.pegasus.planner.code.generator;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** Tests for the Metrics code generator class. */
public class MetricsTest {
    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testMetricsFileSuffixConstant() {
        assertEquals(".metrics", Metrics.METRICS_FILE_SUFFIX);
    }

    @Test
    public void testMetricsServerDefaultURL() {
        assertNotNull(Metrics.METRICS_SERVER_DEFAULT_URL);
        assertTrue(Metrics.METRICS_SERVER_DEFAULT_URL.startsWith("http"));
    }

    @Test
    public void testCollectMetricsEnvVariableConstant() {
        assertEquals("PEGASUS_METRICS", Metrics.COLLECT_METRICS_ENV_VARIABLE);
    }

    @Test
    public void testPrimaryMetricsServerURLEnvVariable() {
        assertEquals("PEGASUS_METRICS_SERVER", Metrics.PRIMARY_METRICS_SERVER_URL_ENV_VARIABLE);
    }

    @Test
    public void testSecondaryMetricsServerURLEnvVariable() {
        assertEquals(
                "PEGASUS_USER_METRICS_SERVER", Metrics.SECONDARY_METRICS_SERVER_URL_ENV_VARIABLE);
    }

    @Test
    public void testDagmanMetricsEnvVariable() {
        assertEquals("PEGASUS_METRICS", Metrics.DAGMAN_METRICS_ENV_VARIABLE);
    }

    @Test
    public void testMetricsSendTimeoutIsPositive() {
        assertTrue(Metrics.METRICS_SEND_TIMEOUT > 0);
    }
}
