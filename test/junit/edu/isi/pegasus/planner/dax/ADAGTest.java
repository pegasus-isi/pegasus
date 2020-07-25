/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.dax;

import static org.junit.Assert.*;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.dax.Invoke.WHEN;
import edu.isi.pegasus.planner.parser.dax.Callback;
import edu.isi.pegasus.planner.parser.dax.DAX2CDAG;
import edu.isi.pegasus.planner.parser.dax.DAXParser5;
import org.junit.Test;

/** @author ryantanaka */
public class ADAGTest {

    @Test
    public void testADAGYamlSerialization() {
        ADAG wf = new ADAG("test");

        wf.addInvoke(WHEN.start, "cmd");

        File fa = new File("f.a");
        fa.addMetaData("foo", "bar");
        fa.addMetaData("num", "1");
        fa.addPhysicalFile("file:///scratch/f.a", "local");
        wf.addFile(fa);

        File checkpoint = new File("f.cp");
        File fb1 = new File("f.b1");
        File fb2 = new File("f.b2");
        File fc1 = new File("f.c1");
        File fc2 = new File("f.c2");
        File fd = new File("f.d");

        Executable preprocess = new Executable("pegasus", "preprocess", "1.0");
        preprocess.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
        preprocess.setInstalled(false);
        preprocess.addPhysicalFile(new PFN("/path", "local"));
        preprocess.addProfile(Profile.NAMESPACE.globus, "walltime", "120");
        preprocess.addMetaData("project", "pegasus");

        Executable findrange = new Executable("pegasus", "findrange", "1.0");
        findrange.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
        findrange.addPhysicalFile(new PFN("/path", "local"));
        findrange.addProfile(Profile.NAMESPACE.globus, "walltime", "120");
        findrange.addMetaData("project", "pegasus");

        Executable analyze = new Executable("pegasus", "analyze", "1.0");
        analyze.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
        analyze.unsetInstalled();
        analyze.addPhysicalFile(new PFN("/path", "local"));
        analyze.addProfile(Profile.NAMESPACE.globus, "walltime", "120");
        analyze.addMetaData("project", "pegasus");
        analyze.addRequirement(preprocess);

        wf.addExecutable(preprocess).addExecutable(findrange).addExecutable(analyze);

        // does nothing
        Transformation diamond = new Transformation("pegasus", "diamond", "1.0");
        diamond.uses(preprocess).uses(findrange).uses(analyze);
        diamond.uses(new File("config", File.LINK.INPUT));
        wf.addTransformation(diamond);

        Job j1 = new Job("j1", "pegasus", "preprocess", "1.0", "j1");
        j1.addArgument("-a preprocess -T 60 -i ").addArgument(fa);
        j1.addArgument("-o ").addArgument(fb1).addArgument(fb2);
        j1.uses(fa, File.LINK.INPUT);
        j1.uses(fb1, File.LINK.OUTPUT);
        j1.uses("f.b2", File.LINK.OUTPUT);
        j1.uses(checkpoint, File.LINK.CHECKPOINT);
        j1.addProfile(Profile.NAMESPACE.dagman, "pre", "20");
        j1.addInvoke(WHEN.start, "cmd");
        j1.addInvoke(WHEN.at_end, "cmd");
        wf.addJob(j1);

        DAG j2 = new DAG("j2", "findrange.dag", "j2");
        j2.uses(new File("f.b1"), File.LINK.INPUT);
        j2.uses("f.c1", File.LINK.OUTPUT, File.TRANSFER.FALSE, false);
        j2.addProfile(Profile.NAMESPACE.dagman, "pre", "20");
        j2.addProfile("condor", "universe", "vanilla");
        wf.addDAG(j2);

        DAX j3 = new DAX("j3", "findrange.dax", "j3");
        j3.addArgument("--site ").addArgument("local");
        j3.uses(new File("f.b2"), File.LINK.INPUT, "");
        j3.uses(new File("f.c2"), File.LINK.OUTPUT, File.TRANSFER.FALSE, false, false, false, "30");
        j3.addProfile("ENV", "SOME_VAR", "SOME_VALUE");
        wf.addDAX(j3);

        Job j4 = new Job("j4", "pegasus", "analyze", "");
        File[] infiles = {fc1, fc2};
        j4.addArgument("-a", "analyze")
                .addArgument("-T")
                .addArgument("60")
                .addArgument("-i", infiles, " ", ",");
        j4.addArgument("-o", fd);
        j4.uses(fc1, File.LINK.INPUT);
        j4.uses(fc2, File.LINK.INPUT);
        j4.uses(fd, File.LINK.OUTPUT);
        wf.addJob(j4);

        wf.addDependency("j1", "j2", "1-2");
        wf.addDependency("j1", "j3", "1-3");
        wf.addDependency("j2", "j4");
        wf.addDependency("j3", "j4");

        String result = wf.toYAML();
        String expected =
                "---\n"
                        + "pegasus: \"5.0\"\n"
                        + "x-pegasus:\n"
                        + "  createdBy: \"ryantanaka\"\n"
                        + "  createdOn: \"2020-07-17T03:58:49Z\"\n"
                        + "  apiLang: \"java\"\n"
                        + "name: \"test\"\n"
                        + "hooks:\n"
                        + "  shell:\n"
                        + "   -\n"
                        + "    _on: \"start\"\n"
                        + "    cmd: \"cmd\"\n"
                        + "metadata:\n"
                        + "  wf.api: \"java\"\n"
                        + "replicaCatalog:\n"
                        + "  replicas:\n"
                        + "   -\n"
                        + "    lfn: \"f.a\"\n"
                        + "    pfns:\n"
                        + "     -\n"
                        + "      pfn: \"file:///scratch/f.a\"\n"
                        + "      site: \"local\"\n"
                        + "    metadata:\n"
                        + "      foo: \"bar\"\n"
                        + "      num: \"1\"\n"
                        + "transformationCatalog:\n"
                        + "  transformations:\n"
                        + "   -\n"
                        + "    namespace: \"pegasus\"\n"
                        + "    name: \"analyze\"\n"
                        + "    version: \"1.0\"\n"
                        + "    requires:\n"
                        + "     - \"pegasus::preprocess:1.0\"\n"
                        + "    sites:\n"
                        + "     -\n"
                        + "      name: \"local\"\n"
                        + "      type: \"stageable\"\n"
                        + "      pfn: \"/path\"\n"
                        + "      bypass: false\n"
                        + "      arch: \"x86\"\n"
                        + "      os.type: \"linux\"\n"
                        + "      profiles:\n"
                        + "        globus:\n"
                        + "          walltime: \"120\"\n"
                        + "      metadata:\n"
                        + "        project: \"pegasus\"\n"
                        + "   -\n"
                        + "    namespace: \"pegasus\"\n"
                        + "    name: \"findrange\"\n"
                        + "    version: \"1.0\"\n"
                        + "    sites:\n"
                        + "     -\n"
                        + "      name: \"local\"\n"
                        + "      type: \"installed\"\n"
                        + "      pfn: \"/path\"\n"
                        + "      bypass: false\n"
                        + "      arch: \"x86\"\n"
                        + "      os.type: \"linux\"\n"
                        + "      profiles:\n"
                        + "        globus:\n"
                        + "          walltime: \"120\"\n"
                        + "      metadata:\n"
                        + "        project: \"pegasus\"\n"
                        + "   -\n"
                        + "    namespace: \"pegasus\"\n"
                        + "    name: \"preprocess\"\n"
                        + "    version: \"1.0\"\n"
                        + "    sites:\n"
                        + "     -\n"
                        + "      name: \"local\"\n"
                        + "      type: \"stageable\"\n"
                        + "      pfn: \"/path\"\n"
                        + "      bypass: false\n"
                        + "      arch: \"x86\"\n"
                        + "      os.type: \"linux\"\n"
                        + "      profiles:\n"
                        + "        globus:\n"
                        + "          walltime: \"120\"\n"
                        + "      metadata:\n"
                        + "        project: \"pegasus\"\n"
                        + "jobs:\n"
                        + " -\n"
                        + "  type: \"job\"\n"
                        + "  name: \"preprocess\"\n"
                        + "  namespace: \"pegasus\"\n"
                        + "  version: \"1.0\"\n"
                        + "  id: \"j1\"\n"
                        + "  nodeLabel: \"j1\"\n"
                        + "  profiles:\n"
                        + "    dagman:\n"
                        + "      pre: \"20\"\n"
                        + "  hooks:\n"
                        + "    shell:\n"
                        + "     -\n"
                        + "      _on: \"start\"\n"
                        + "      cmd: \"cmd\"\n"
                        + "     -\n"
                        + "      _on: \"end\"\n"
                        + "      cmd: \"cmd\"\n"
                        + "  arguments:\n"
                        + "   - \"-a preprocess -T 60 -i \"\n"
                        + "   - \" \"\n"
                        + "   - \"f.a\"\n"
                        + "   - \" \"\n"
                        + "   - \"-o \"\n"
                        + "   - \" \"\n"
                        + "   - \"f.b1\"\n"
                        + "   - \" \"\n"
                        + "   - \"f.b2\"\n"
                        + "  uses:\n"
                        + "   -\n"
                        + "    lfn: \"f.a\"\n"
                        + "    type: \"input\"\n"
                        + "    stageOut: true\n"
                        + "    registerReplica: true\n"
                        + "   -\n"
                        + "    lfn: \"f.b1\"\n"
                        + "    type: \"output\"\n"
                        + "    stageOut: true\n"
                        + "    registerReplica: true\n"
                        + "   -\n"
                        + "    lfn: \"f.b2\"\n"
                        + "    type: \"output\"\n"
                        + "    stageOut: true\n"
                        + "    registerReplica: true\n"
                        + "   -\n"
                        + "    lfn: \"f.cp\"\n"
                        + "    type: \"checkpoint\"\n"
                        + "    stageOut: true\n"
                        + "    registerReplica: true\n"
                        + " -\n"
                        + "  type: \"condorWorkflow\"\n"
                        + "  file: \"findrange.dag\"\n"
                        + "  id: \"j2\"\n"
                        + "  nodeLabel: \"j2\"\n"
                        + "  profiles:\n"
                        + "    condor:\n"
                        + "      universe: \"vanilla\"\n"
                        + "    dagman:\n"
                        + "      pre: \"20\"\n"
                        + "  arguments: []\n"
                        + "  uses:\n"
                        + "   -\n"
                        + "    lfn: \"f.b1\"\n"
                        + "    type: \"input\"\n"
                        + "    stageOut: true\n"
                        + "    registerReplica: true\n"
                        + "   -\n"
                        + "    lfn: \"f.c1\"\n"
                        + "    type: \"output\"\n"
                        + "    stageOut: false\n"
                        + "    registerReplica: false\n"
                        + " -\n"
                        + "  type: \"pegasusWorkflow\"\n"
                        + "  file: \"findrange.dax\"\n"
                        + "  id: \"j3\"\n"
                        + "  nodeLabel: \"j3\"\n"
                        + "  profiles:\n"
                        + "    env:\n"
                        + "      SOME_VAR: \"SOME_VALUE\"\n"
                        + "  arguments:\n"
                        + "   - \"--site \"\n"
                        + "   - \" \"\n"
                        + "   - \"local\"\n"
                        + "  uses:\n"
                        + "   -\n"
                        + "    lfn: \"f.b2\"\n"
                        + "    type: \"input\"\n"
                        + "    stageOut: true\n"
                        + "    registerReplica: true\n"
                        + "   -\n"
                        + "    lfn: \"f.c2\"\n"
                        + "    type: \"output\"\n"
                        + "    stageOut: false\n"
                        + "    registerReplica: false\n"
                        + "    size: \"30\"\n"
                        + " -\n"
                        + "  type: \"job\"\n"
                        + "  name: \"analyze\"\n"
                        + "  namespace: \"pegasus\"\n"
                        + "  id: \"j4\"\n"
                        + "  arguments:\n"
                        + "   - \"-a analyze\"\n"
                        + "   - \" \"\n"
                        + "   - \"-T\"\n"
                        + "   - \" \"\n"
                        + "   - \"60\"\n"
                        + "   - \" \"\n"
                        + "   - \"-i \"\n"
                        + "   - \"f.c1\"\n"
                        + "   - \",\"\n"
                        + "   - \"f.c2\"\n"
                        + "   - \" \"\n"
                        + "   - \"-o \"\n"
                        + "   - \"f.d\"\n"
                        + "  uses:\n"
                        + "   -\n"
                        + "    lfn: \"f.c1\"\n"
                        + "    type: \"input\"\n"
                        + "    stageOut: true\n"
                        + "    registerReplica: true\n"
                        + "   -\n"
                        + "    lfn: \"f.c2\"\n"
                        + "    type: \"input\"\n"
                        + "    stageOut: true\n"
                        + "    registerReplica: true\n"
                        + "   -\n"
                        + "    lfn: \"f.d\"\n"
                        + "    type: \"output\"\n"
                        + "    stageOut: true\n"
                        + "    registerReplica: true\n"
                        + "jobDependencies:\n"
                        + " -\n"
                        + "  id: \"j1\"\n"
                        + "  children:\n"
                        + "   - \"j2\"\n"
                        + "   - \"j3\"\n"
                        + " -\n"
                        + "  id: \"j2\"\n"
                        + "  children:\n"
                        + "   - \"j4\"\n"
                        + " -\n"
                        + "  id: \"j3\"\n"
                        + "  children:\n"
                        + "   - \"j4\"\n";

        // use a fixed "createdOn" value for test
        String createdOn = "\"today\"";
        String pattern1 = "\"\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}Z\"";
        result = result.replaceAll(pattern1, createdOn);
        expected = expected.replaceAll(pattern1, createdOn);

        // use a fixed "createdBy" value for test
        String createdBy = "createdBy: \"bamboo\"";
        String pattern2 = "createdBy: \\p{Print}+";
        result = result.replaceAll(pattern2, createdBy);
        expected = expected.replaceAll(pattern2, createdBy);

        System.out.println(result);

        assertEquals(result, expected);

        // validate against schema
        wf.writeToFile("/tmp/diamond.yml", ADAG.FORMAT.yaml);
        Callback c = new DAX2CDAG();
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, LogManager.getInstance("", ""));
        DAXParser5 parser = new DAXParser5(bag, "5.0");
        c.initialize(bag, "/tmp/diamond.yml");

        boolean isValid = parser.validate("/tmp/diamond.yml");

        System.err.println("Validation of file: " + parser.validate("/tmp/diamond.yml"));

        assertEquals(isValid, true);
    }
}
