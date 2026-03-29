/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.classes;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.planner.namespace.Metadata;
import java.io.IOException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** @author vahi */
public class PegasusFileTest {

    public PegasusFileTest() {}

    @BeforeAll
    public static void setUpClass() {}

    @AfterAll
    public static void tearDownClass() {}

    @BeforeEach
    public void setUp() {}

    @AfterEach
    public void tearDown() {}

    @Test
    public void testSimpleInputUsesCreation() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "lfn: f.b2\n" + "size: 2048\n" + "type: input\n";

        PegasusFile pf = mapper.readValue(test, PegasusFile.class);
        assertNotNull(pf);
        assertEquals("f.b2", pf.getLFN());
        assertEquals(2048, (int) pf.getSize());
        assertEquals("input", pf.getLinkage().toString());
    }

    @Test
    public void testSimpleOutputUsesCreation() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "lfn: f.b2\n" + "size: 2048\n" + "type: output\n";

        PegasusFile pf = mapper.readValue(test, PegasusFile.class);
        assertNotNull(pf);
        assertEquals("f.b2", pf.getLFN());
        assertEquals(2048, (int) pf.getSize());
        assertEquals("output", pf.getLinkage().toString());
    }

    @Test
    public void testUsesStageout() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "lfn: f.b2\n" + "stageOut: true\n" + "type: output\n";

        PegasusFile pf = mapper.readValue(test, PegasusFile.class);
        assertNotNull(pf);
        assertEquals("f.b2", pf.getLFN());
        assertFalse(pf.doBypassStaging());
        assertTrue(!pf.getTransientTransferFlag());
        assertEquals("output", pf.getLinkage().toString());
    }

    @Test
    public void testUsesBypass() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "lfn: f.b2\n" + "bypass: true\n" + "type: input\n";

        PegasusFile pf = mapper.readValue(test, PegasusFile.class);
        assertNotNull(pf);
        assertEquals("f.b2", pf.getLFN());
        assertTrue(pf.doBypassStaging());
        assertEquals("input", pf.getLinkage().toString());
    }

    @Test
    public void testUsesRegistration() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "lfn: f.b2\n" + "registerReplica: true\n" + "type: output\n";

        PegasusFile pf = mapper.readValue(test, PegasusFile.class);
        assertNotNull(pf);
        assertEquals("f.b2", pf.getLFN());
        assertTrue(pf.getRegisterFlag());
        assertEquals("output", pf.getLinkage().toString());
    }

    @Test
    public void testUsesForPlanningFlag() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "lfn: f.b2\n" + "forPlanning: true\n" + "type: output\n";

        PegasusFile pf = mapper.readValue(test, PegasusFile.class);
        assertNotNull(pf);
        assertEquals("f.b2", pf.getLFN());
        assertTrue(pf.useForPlanning());
        assertEquals("output", pf.getLinkage().toString());
    }

    @Test
    public void testUsesMetadata() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "lfn: f.b2\n" + "stageOut: true\n" + "metadata:\n" + "  createdBy: vahi";

        PegasusFile pf = mapper.readValue(test, PegasusFile.class);
        assertNotNull(pf);
        assertEquals("f.b2", pf.getLFN());
        assertEquals(true, !pf.getTransientTransferFlag());

        Metadata m = pf.getAllMetadata();
        assertTrue(m.containsKey("createdBy"));
        assertEquals("vahi", (String) m.get("createdBy"));
    }
}
