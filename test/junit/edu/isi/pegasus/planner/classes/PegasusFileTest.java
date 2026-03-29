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
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
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

    // ---- Additional tests ----

    @Test
    public void testDefaultConstructorLFN() {
        PegasusFile pf = new PegasusFile();
        assertEquals("", pf.getLFN());
    }

    @Test
    public void testOverloadedConstructorLFN() {
        PegasusFile pf = new PegasusFile("input.txt");
        assertEquals("input.txt", pf.getLFN());
    }

    @Test
    public void testSetAndGetLFN() {
        PegasusFile pf = new PegasusFile();
        pf.setLFN("output.dat");
        assertEquals("output.dat", pf.getLFN());
    }

    @Test
    public void testDefaultTypeIsData() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertEquals(PegasusFile.DATA_FILE, pf.getType());
        assertTrue(pf.isDataFile());
        assertFalse(pf.isExecutable());
        assertFalse(pf.isCheckpointFile());
    }

    @Test
    public void testSetTypeExecutable() {
        PegasusFile pf = new PegasusFile("binary");
        pf.setType(PegasusFile.EXECUTABLE_FILE);
        assertEquals(PegasusFile.EXECUTABLE_FILE, pf.getType());
        assertTrue(pf.isExecutable());
        assertFalse(pf.isDataFile());
    }

    @Test
    public void testSetTypeByStringExecutable() {
        PegasusFile pf = new PegasusFile("binary");
        pf.setType(PegasusFile.EXECUTABLE_TYPE);
        assertTrue(pf.isExecutable());
        assertEquals(PegasusFile.EXECUTABLE_TYPE, pf.getTypeAsString());
    }

    @Test
    public void testSetTypeCheckpoint() {
        PegasusFile pf = new PegasusFile("chk.dat");
        pf.setType(PegasusFile.CHECKPOINT_FILE);
        assertTrue(pf.isCheckpointFile());
        // checkpoint files are automatically optional
        assertTrue(pf.fileOptional());
    }

    @Test
    public void testSetTypeByStringCheckpoint() {
        PegasusFile pf = new PegasusFile("chk.dat");
        pf.setType(PegasusFile.CHECKPOINT_TYPE);
        assertTrue(pf.isCheckpointFile());
        assertTrue(pf.fileOptional());
    }

    @Test
    public void testSetTypeDocker() {
        PegasusFile pf = new PegasusFile("container.tar");
        pf.setType(PegasusFile.DOCKER_CONTAINER_FILE);
        assertTrue(pf.isContainerFile());
        assertEquals(PegasusFile.DOCKER_TYPE, pf.getTypeAsString());
    }

    @Test
    public void testSetTypeSingularity() {
        PegasusFile pf = new PegasusFile("container.sif");
        pf.setType(PegasusFile.SINGULARITY_CONTAINER_FILE);
        assertTrue(pf.isContainerFile());
        assertEquals(PegasusFile.SINGULARITY_TYPE, pf.getTypeAsString());
    }

    @Test
    public void testSetTypeShifter() {
        PegasusFile pf = new PegasusFile("container.img");
        pf.setType(PegasusFile.SHIFTER_CONTAINER_FILE);
        assertTrue(pf.isContainerFile());
        assertEquals(PegasusFile.SHIFTER_TYPE, pf.getTypeAsString());
    }

    @Test
    public void testSetTypeContainerEnum() {
        PegasusFile pf = new PegasusFile("container.tar");
        pf.setType(Container.TYPE.docker);
        assertTrue(pf.isContainerFile());
        assertEquals(PegasusFile.DOCKER_CONTAINER_FILE, pf.getType());
    }

    @Test
    public void testSetTypeInvalidThrowsException() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertThrows(IllegalArgumentException.class, () -> pf.setType(99));
    }

    @Test
    public void testSetTypeInvalidStringThrowsException() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertThrows(IllegalArgumentException.class, () -> pf.setType("invalid-type"));
    }

    @Test
    public void testSetTypeNullStringThrowsException() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertThrows(IllegalArgumentException.class, () -> pf.setType((String) null));
    }

    @Test
    public void testDefaultTransferFlagIsMandatory() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertEquals(PegasusFile.TRANSFER_MANDATORY, pf.getTransferFlag());
        assertFalse(pf.getTransientTransferFlag());
    }

    @Test
    public void testSetTransferFlagNot() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setTransferFlag(PegasusFile.TRANSFER_NOT);
        assertEquals(PegasusFile.TRANSFER_NOT, pf.getTransferFlag());
        assertTrue(pf.getTransientTransferFlag());
    }

    @Test
    public void testSetTransferFlagOptional() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setTransferFlag(PegasusFile.TRANSFER_OPTIONAL);
        assertEquals(PegasusFile.TRANSFER_OPTIONAL, pf.getTransferFlag());
        assertFalse(pf.getTransientTransferFlag());
    }

    @Test
    public void testSetTransferFlagBooleanFalse() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setTransferFlag(false);
        assertEquals(PegasusFile.TRANSFER_NOT, pf.getTransferFlag());
    }

    @Test
    public void testSetTransferFlagBooleanTrue() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setTransferFlag(false); // set to NOT first
        pf.setTransferFlag(true); // reset to MANDATORY
        assertEquals(PegasusFile.TRANSFER_MANDATORY, pf.getTransferFlag());
    }

    @Test
    public void testSetTransferFlagStringOptional() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setTransferFlag("optional");
        assertEquals(PegasusFile.TRANSFER_OPTIONAL, pf.getTransferFlag());
    }

    @Test
    public void testSetTransferFlagStringTrueDoubleNegativeFalse() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setTransferFlag("true", false);
        assertEquals(PegasusFile.TRANSFER_MANDATORY, pf.getTransferFlag());
    }

    @Test
    public void testSetTransferFlagStringFalseDoubleNegativeTrue() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setTransferFlag("false", true);
        assertEquals(PegasusFile.TRANSFER_MANDATORY, pf.getTransferFlag());
    }

    @Test
    public void testSetTransferFlagStringTrueDoubleNegativeTrue() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setTransferFlag("true", true);
        assertEquals(PegasusFile.TRANSFER_NOT, pf.getTransferFlag());
    }

    @Test
    public void testSetTransferFlagInvalidStringThrowsException() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertThrows(IllegalArgumentException.class, () -> pf.setTransferFlag("invalid"));
    }

    @Test
    public void testSetTransferFlagOutOfRangeThrowsException() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertThrows(IllegalArgumentException.class, () -> pf.setTransferFlag(99));
    }

    @Test
    public void testDefaultSizeIsMinusOne() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertEquals(-1.0, pf.getSize(), 0.001);
    }

    @Test
    public void testSetSizeDouble() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setSize(1024.0);
        assertEquals(1024.0, pf.getSize(), 0.001);
    }

    @Test
    public void testSetSizeString() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setSize("4096");
        assertEquals(4096.0, pf.getSize(), 0.001);
    }

    @Test
    public void testSetSizeStringNull() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setSize(100.0);
        pf.setSize((String) null);
        assertEquals(-1.0, pf.getSize(), 0.001);
    }

    @Test
    public void testDefaultCleanupFlagIsTrue() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertTrue(pf.canBeCleanedup());
    }

    @Test
    public void testSetForCleanupFalse() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setForCleanup(false);
        assertFalse(pf.canBeCleanedup());
    }

    @Test
    public void testSetForCleanupTrue() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setForCleanup(false);
        pf.setForCleanup(true);
        assertTrue(pf.canBeCleanedup());
    }

    @Test
    public void testDefaultIntegrityFlagIsTrue() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertTrue(pf.doIntegrityChecking());
    }

    @Test
    public void testSetForIntegrityCheckingFalse() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setForIntegrityChecking(false);
        assertFalse(pf.doIntegrityChecking());
    }

    @Test
    public void testDefaultBypassStagingIsFalse() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertFalse(pf.doBypassStaging());
    }

    @Test
    public void testSetForBypassStaging() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setForBypassStaging();
        assertTrue(pf.doBypassStaging());
    }

    @Test
    public void testSetForBypassStagingFalse() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setForBypassStaging(true);
        pf.setForBypassStaging(false);
        assertFalse(pf.doBypassStaging());
    }

    @Test
    public void testDefaultOptionalFlagIsFalse() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertFalse(pf.fileOptional());
    }

    @Test
    public void testSetFileOptional() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setFileOptional();
        assertTrue(pf.fileOptional());
    }

    @Test
    public void testSetFileOptionalFalse() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setFileOptional(true);
        pf.setFileOptional(false);
        assertFalse(pf.fileOptional());
    }

    @Test
    public void testDefaultPlanningUseFlagIsFalse() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertFalse(pf.useForPlanning());
    }

    @Test
    public void testSetUseForPlanning() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setUseForPlanning();
        assertTrue(pf.useForPlanning());
    }

    @Test
    public void testDefaultRegisterFlagIsTrue() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertTrue(pf.getRegisterFlag());
        assertFalse(pf.getTransientRegFlag());
    }

    @Test
    public void testSetRegisterFlagFalse() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setRegisterFlag(false);
        assertFalse(pf.getRegisterFlag());
        assertTrue(pf.getTransientRegFlag());
    }

    @Test
    public void testDefaultLinkageIsNone() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertEquals(PegasusFile.LINKAGE.none, pf.getLinkage());
    }

    @Test
    public void testSetLinkageInput() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setLinkage(PegasusFile.LINKAGE.input);
        assertEquals(PegasusFile.LINKAGE.input, pf.getLinkage());
    }

    @Test
    public void testSetLinkageOutput() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setLinkage(PegasusFile.LINKAGE.output);
        assertEquals(PegasusFile.LINKAGE.output, pf.getLinkage());
    }

    @Test
    public void testSetLinkageInout() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setLinkage(PegasusFile.LINKAGE.inout);
        assertEquals(PegasusFile.LINKAGE.inout, pf.getLinkage());
    }

    @Test
    public void testAddMetadataKeyValue() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.addMetadata("creator", "pegasus");
        assertEquals("pegasus", pf.getMetadata("creator"));
    }

    @Test
    public void testAddMetadataObject() {
        PegasusFile pf = new PegasusFile("f.txt");
        Metadata m = new Metadata();
        m.checkKeyInNS("project", "science");
        pf.addMetadata(m);
        assertEquals("science", pf.getMetadata("project"));
    }

    @Test
    public void testGetAllMetadataNotNull() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertNotNull(pf.getAllMetadata());
    }

    @Test
    public void testSetAndGetMetadata() {
        PegasusFile pf = new PegasusFile("f.txt");
        Metadata m = new Metadata();
        m.checkKeyInNS("key1", "value1");
        pf.setMetadata(m);
        assertEquals("value1", pf.getMetadata("key1"));
    }

    @Test
    public void testEqualsWithSameLFN() {
        PegasusFile pf1 = new PegasusFile("data.txt");
        PegasusFile pf2 = new PegasusFile("data.txt");
        assertEquals(pf1, pf2);
    }

    @Test
    public void testNotEqualsWithDifferentLFN() {
        PegasusFile pf1 = new PegasusFile("data.txt");
        PegasusFile pf2 = new PegasusFile("other.txt");
        assertNotEquals(pf1, pf2);
    }

    @Test
    public void testHashCodeConsistency() {
        PegasusFile pf1 = new PegasusFile("data.txt");
        PegasusFile pf2 = new PegasusFile("data.txt");
        assertEquals(pf1.hashCode(), pf2.hashCode());
    }

    @Test
    public void testClonePreservesLFN() {
        PegasusFile pf = new PegasusFile("original.txt");
        pf.setType(PegasusFile.EXECUTABLE_FILE);
        pf.setSize(512.0);
        PegasusFile clone = (PegasusFile) pf.clone();
        assertEquals("original.txt", clone.getLFN());
        assertEquals(PegasusFile.EXECUTABLE_FILE, clone.getType());
        assertEquals(512.0, clone.getSize(), 0.001);
    }

    @Test
    public void testCloneIsIndependent() {
        PegasusFile pf = new PegasusFile("original.txt");
        PegasusFile clone = (PegasusFile) pf.clone();
        clone.setLFN("changed.txt");
        assertEquals("original.txt", pf.getLFN());
    }

    @Test
    public void testDefaultIsRawInputFalse() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertFalse(pf.isRawInputFile());
    }

    @Test
    public void testSetRawInput() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setRawInput(true);
        assertTrue(pf.isRawInputFile());
    }

    @Test
    public void testDefaultChecksumComputedInWFFalse() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertFalse(pf.hasChecksumComputedInWF());
    }

    @Test
    public void testSetChecksumComputedInWF() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setChecksumComputedInWF(true);
        assertTrue(pf.hasChecksumComputedInWF());
    }

    @Test
    public void testHasRCCheckSumFalseByDefault() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertFalse(pf.hasRCCheckSum());
    }

    @Test
    public void testHasRCCheckSumTrueWhenMetadataSet() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.addMetadata(Metadata.CHECKSUM_VALUE_KEY, "abc123");
        assertTrue(pf.hasRCCheckSum());
    }

    @Test
    public void testTypeValidRange() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertTrue(pf.typeValid(PegasusFile.DATA_FILE));
        assertTrue(pf.typeValid(PegasusFile.EXECUTABLE_FILE));
        assertTrue(pf.typeValid(PegasusFile.CHECKPOINT_FILE));
        assertTrue(pf.typeValid(PegasusFile.DOCKER_CONTAINER_FILE));
        assertTrue(pf.typeValid(PegasusFile.SINGULARITY_CONTAINER_FILE));
        assertTrue(pf.typeValid(PegasusFile.SHIFTER_CONTAINER_FILE));
        assertTrue(pf.typeValid(PegasusFile.OTHER_FILE));
        assertFalse(pf.typeValid(-1));
        assertFalse(pf.typeValid(99));
    }

    @Test
    public void testTransferInRange() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertTrue(pf.transferInRange(PegasusFile.TRANSFER_MANDATORY));
        assertTrue(pf.transferInRange(PegasusFile.TRANSFER_OPTIONAL));
        assertTrue(pf.transferInRange(PegasusFile.TRANSFER_NOT));
        assertFalse(pf.transferInRange(-1));
        assertFalse(pf.transferInRange(99));
    }

    @Test
    public void testTypeToStringData() {
        PegasusFile pf = new PegasusFile("f.txt");
        assertEquals(PegasusFile.DATA_TYPE, pf.typeToString());
    }

    @Test
    public void testTypeToStringExecutable() {
        PegasusFile pf = new PegasusFile("f.txt");
        pf.setType(PegasusFile.EXECUTABLE_FILE);
        assertEquals(PegasusFile.EXECUTABLE_TYPE, pf.typeToString());
    }

    @Test
    public void testToStringContainsLFN() {
        PegasusFile pf = new PegasusFile("myfile.dat");
        String s = pf.toString();
        assertTrue(s.contains("myfile.dat"));
    }

    @Test
    public void testCheckpointUsesDeserializationSetsLinkageInout() throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);

        String test = "lfn: chk.dat\n" + "type: checkpoint\n";

        PegasusFile pf = mapper.readValue(test, PegasusFile.class);
        assertNotNull(pf);
        assertTrue(pf.isCheckpointFile());
        assertEquals(PegasusFile.LINKAGE.inout, pf.getLinkage());
    }
}
