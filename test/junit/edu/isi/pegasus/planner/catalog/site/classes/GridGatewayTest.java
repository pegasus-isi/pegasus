/*
 * Copyright 2007-20120 University Of Southern California
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.isi.pegasus.planner.catalog.site.classes;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway.TYPE;
import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author Karan Vahi */
public class GridGatewayTest {

    public GridGatewayTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    @Before
    public void setUp() {}

    @After
    public void tearDown() {}

    @Test
    public void testGridGatewaySerialization() throws IOException {
        GridGateway gw = new GridGateway();
        gw.setArchitecture(SysInfo.Architecture.x86_64);
        gw.setIdleNodes(3);
        gw.setTotalNodes(40);
        gw.setContact("smarty.isi.edu/jobmanager-pbs");
        gw.setJobType(GridGateway.JOB_TYPE.compute);
        gw.setScheduler(GridGateway.SCHEDULER_TYPE.pbs);
        ObjectMapper mapper =
                new ObjectMapper(
                        new YAMLFactory().configure(YAMLGenerator.Feature.INDENT_ARRAYS, true));
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
        String expected =
                "---\n"
                        + "type: \"gt5\"\n"
                        + "contact: \"smarty.isi.edu/jobmanager-pbs\"\n"
                        + "scheduler: \"pbs\"\n"
                        + "jobtype: \"compute\"\n"
                        + "idleNodes: 3\n"
                        + "totalNodes: 40\n";
        String actual = mapper.writeValueAsString(gw);
        assertEquals(expected, actual);
    }

    @Test
    public void testGridGatewayDeserialization() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test =
                "type: gt5\n"
                        + "contact: smarty.isi.edu/jobmanager-pbs\n"
                        + "scheduler: pbs\n"
                        + "jobtype: auxillary";

        GridGateway gw = mapper.readValue(test, GridGateway.class);
        assertNotNull(gw);
        assertEquals(TYPE.gt5, gw.getType());
        assertEquals("smarty.isi.edu/jobmanager-pbs", gw.getContact());
        assertEquals(GridGateway.SCHEDULER_TYPE.pbs, gw.getScheduler());
        assertEquals(GridGateway.JOB_TYPE.auxillary, gw.getJobType());
    }
}
