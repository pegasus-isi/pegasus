/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.classes;

import static org.junit.Assert.*;

import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import edu.isi.pegasus.planner.namespace.Metadata;
import java.io.IOException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/** @author vahi */
public class PegasusFileTest {

    public PegasusFileTest() {}

    @BeforeClass
    public static void setUpClass() {}

    @AfterClass
    public static void tearDownClass() {}

    @Before
    public void setUp() {}

    @After
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
        assertTrue(!pf.getTransientTransferFlag());
        assertEquals("output", pf.getLinkage().toString());
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
