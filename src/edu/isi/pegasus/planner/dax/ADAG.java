/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package edu.isi.pegasus.planner.dax;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;
import java.io.Writer;
import java.io.FileWriter;
import java.io.IOException;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.common.util.XMLWriter;

/**
 * <pre>
DAX Generator for Pegasus. The DAX SCHEMA is available at http://pegasus.isi.edu/schema/dax-3.2.xsd
and documentation available at http://pegasus.isi.edu/wms/doc.php
To generate an example DIAMOND DAX run the ADAG Class as shown below
java ADAG <filename>

Shown below is the Code for generating the DIAMOND DAX.
NOTE: This is an illustrative example only. Please see examples directory for a working example

create a new ADAG object

@see ADAG
ADAG dax = new ADAG("test");

create a File object
 @see File
File fa = new File("f.a");

 //add MetaData entry to the file objects
 @see MetaData
fa.addMetaData("string", "foo", "bar");
fa.addMetaData("int", "num", "1");

 // add Profile entry to the file objects
 @see Profile
fa.addProfile("env", "FOO", "/usr/bar");
fa.addProfile("globus", "walltime", "40");

 //add PFN to the File object
 @see PFN
fa.addPhysicalFile("file:///scratch/f.a", "local");

 //add the File object to the Replica Catalog section of the DAX
 dax.addFile(fa);


File fb1 = new File("f.b1");
fb1.addMetaData("string", "foo", "bar");
fb1.addMetaData("int", "num", "2");
fb1.addProfile("env", "GOO", "/usr/foo");
fb1.addProfile("globus", "walltime", "40");
dax.addFile(fb1);

File fb2 = new File("f.b2");
fb2.addMetaData("string", "foo", "bar");
fb2.addMetaData("int", "num", "3");
fb2.addProfile("env", "BAR", "/usr/goo");
fb2.addProfile("globus", "walltime", "40");
dax.addFile(fb2);

File fc1 = new File("f.c1");
fc1.addProfile("env", "TEST", "/usr/bin/true");
fc1.addProfile("globus", "walltime", "40");
dax.addFile(fc1);

File fc2 = new File("f.c2");
fc2.addMetaData("string", "foo", "bar");
fc2.addMetaData("int", "num", "5");
dax.addFile(fc2);

File fd = new File("f.d");
dax.addFile(fd);

//Create an Executable object
 @see Executable
Executable preprocess = new Executable("pegasus", "preproces", "1.0");
preprocess.setArchitecture(Executable.ARCH.x86).setOS(Executable.OS.LINUX);
preprocess.unsetInstalled();
preprocess.addPhysicaFile(new PFN("file:///opt/pegasus/default/bin/keg"));
preprocess.addProfile(Profile.NAMESPACE.globus, "walltime", "120");
preprocess.addMetaData("string", "project", "pegasus");


Executable findrange = new Executable("pegasus", "findrange", "1.0");
findrange.setArchitecture(Executable.ARCH.x86).setOS(Executable.OS.LINUX);
findrange.unsetInstalled();
findrange.addPhysicaFile(new PFN("http://pegasus.isi.edu/code/bin/keg"));
findrange.addProfile(Profile.NAMESPACE.globus, "walltime", "120");
findrange.addMetaData("string", "project", "pegasus");


Executable analyze = new Executable("pegasus", "analyze", "1.0");
analyze.setArchitecture(Executable.ARCH.x86).setOS(Executable.OS.LINUX);
analyze.unsetInstalled();
analyze.addPhysicaFile(new PFN("gsiftp://localhost/opt/pegasus/default/bin/keg"));
analyze.addProfile(Profile.NAMESPACE.globus, "walltime", "120");
analyze.addMetaData("string", "project", "pegasus");

//add all the executables to the DAX's Tranformation Catalog Section

dax.addExecutable(preprocess).addExecutable(findrange).addExecutable(analyze);

//Create a compound Executable (Exectuable depending on other executable and files)
 @see Transformation
Transformation diamond = new Transformation("pegasus", "diamond", "1.0");
diamond.uses(preprocess).uses(findrange).uses(analyze);
diamond.uses(new File("config", File.LINK.INPUT));

dax.addTransformation(diamond);


Job j1 = new Job("j1", "pegasus", "preprocess", "1.0", "j1");
j1.addArgument("-a preprocess -T 60 -i ").addArgument(fa);
j1.addArgument("-o ").addArgument(fb1).addArgument(fb2);
j1.uses(fa, File.LINK.INPUT);
j1.uses(fb1, File.LINK.OUTPUT);
j1.uses(new File("f.b2"), File.LINK.OUTPUT);
j1.addProfile(Profile.NAMESPACE.dagman, "pre", "20");
dax.addJob(j1);

DAG j2 = new DAG("j2", "findrange.dag", "j2");
j2.uses(new File("f.b1"), File.LINK.INPUT);
j2.uses(new File("f.c1"), File.LINK.OUTPUT);
j2.addProfile(Profile.NAMESPACE.dagman, "pre", "20");
j2.addProfile("condor", "universe", "vanilla");
dax.addDAG(j2);

DAX j3 = new DAX("j3", "findrange.dax", "j3");
j3.addArgument("--site ").addArgument("local");
j3.uses(new File("f.b2"), File.LINK.INPUT);
j3.uses(new File("f.c2"), File.LINK.OUTPUT);
j3.addInvoke(Invoke.WHEN.start, "/bin/notify -m START gmehta@isi.edu");
j3.addInvoke(Invoke.WHEN.at_end, "/bin/notify -m END gmehta@isi.edu");
j3.addProfile("ENV", "HAHA", "YADAYADAYADA");
dax.addDAX(j3);

Job j4 = new Job("j4", "pegasus", "analyze", "");
j4.addArgument("-a analyze -T 60 -i ").addArgument(fc1);
j4.addArgument(" ").addArgument(fc2);
j4.addArgument("-o ").addArgument(fd);
j4.uses(fc1, File.LINK.INPUT);
j4.uses(fc2, File.LINK.INPUT);
j4.uses(fd, File.LINK.OUTPUT);
dax.addJob(j4);

dax.addDependency("j1", "j2", "1-2");
dax.addDependency("j1", "j3", "1-3");
dax.addDependency("j2", "j4");
dax.addDependency("j3", "j4");

//Finally write the dax to a file
dax.writeToFile("diamond.dax");
</pre>
 *
 *
 * @author Gaurang Mehta gmehta at isi dot edu
 */
public class ADAG {

    /**
     * The "official" namespace URI of the site catalog schema.
     */
    public static final String SCHEMA_NAMESPACE = "http://pegasus.isi.edu/schema/DAX";
    /**
     * XSI SCHEMA NAMESPACE
     */
    public static final String SCHEMA_NAMESPACE_XSI = "http://www.w3.org/2001/XMLSchema-instance";
    /**
     * The "not-so-official" location URL of the DAX schema definition.
     */
    public static final String SCHEMA_LOCATION = "http://pegasus.isi.edu/schema/dax-3.2.xsd";
    /**
     * The version to report.
     */
    public static final String SCHEMA_VERSION = "3.2";
    /**
     *  The Name / Label of the DAX
     */
    private String mName;
    /**
     *  The Index of the dax object. I out of N
     */
    private int mIndex;
    /**
     * The Count of the number of dax objects : N
     */
    private int mCount;
    /**
     *  The List of Job,DAX and DAG objects
     * @see DAG
     * @see DAX
     * @see Job
     * @see AbstractJob
     */
    private List<AbstractJob> mJobs;
    /**
     * The List of Transformation objects
     * @see Transformation
     */
    private List<Transformation> mTransformations;
    /**
     * The list of Executable objects
     * @see Executable
     */
    private List<Executable> mExecutables;
    /**
     * The list of edu.isi.pegasus.planner.dax.File objects
     * @see File
     */
    private List<File> mFiles;
    /**
     * Map of Dependencies between Job,DAX,DAG objects.
     * Map key is a string that holds the child element reference, the value is a List of Parent objects
     * @see Parent
     */
    private Map<String, List<Parent>> mDependencies;
    /**
     *  Handle the XML writer
     */
    private XMLWriter mWriter;
    private LogManager mLogger;

    /**
     * The Simple constructor for the DAX object
     * @param name DAX LABEL
     */
    public ADAG(String name) {
        this(name, 0, 1);
    }

    /**
     * DAX Constructor
     * @param name  DAX Label
     * @param index  Index of DAX out of N DAX's
     * @param count  Number of DAXS in a group
     */
    public ADAG(String name, int index, int count) {
        //initialize everything
        mName = name;
        mIndex = index;
        mCount = count;
        mJobs = new LinkedList<AbstractJob>();
        mTransformations = new LinkedList<Transformation>();
        mExecutables = new LinkedList<Executable>();
        mFiles = new LinkedList<File>();
        mDependencies = new HashMap<String, List<Parent>>();
        System.setProperty("pegasus.home", System.getProperty("user.dir"));
        mLogger = LogManagerFactory.loadSingletonInstance();
        mLogger.logEventStart("event.dax.generate", "pegasus.version", Version.instance().toString());

    }

    /**
     * Add a RC File object to the top of the DAX.
     * @param file File object to be added to the RC section
     * @return ADAG
     * @see File
     */
    public ADAG addFile(File file) {
        mFiles.add(file);
        return this;
    }

    /**
     * Add Files to the RC Section on top of the DAX
     * @param files List<File> List of file objects to be added to the RC Section
     * @return ADAG
     * @see File
     *
     */
    public ADAG addFiles(List<File> files) {
        mFiles.addAll(files);
        return this;
    }

    /**
     * Add Executable to the DAX
     * @param executable Executable to be added
     * @return ADAG
     * @see Executable
     */
    public ADAG addExecutable(Executable executable) {
        mExecutables.add(executable);
        return this;
    }

    /**
     * Add Multiple Executable objects to the DAX
     * @param executables List of Executable objects to be added
     * @return ADAG
     * @see Executable
     */
    public ADAG addExecutables(List<Executable> executables) {
        mExecutables.addAll(executables);
        return this;
    }

    /**
     * Add Transformation to the DAX
     * @param transformation Transformation object to be added
     * @return ADAG
     * @see Transformation
     */
    public ADAG addTransformation(Transformation transformation) {
        mTransformations.add(transformation);
        return this;
    }

    /**
     * Add Multiple Transformation to the DAX
     * @param transformations List of Transformation objects
     * @return ADAG
     * @see Transformation
     */
    public ADAG addTransformations(List<Transformation> transformations) {
        mTransformations.addAll(transformations);
        return this;
    }

    /**
     * Add Job to the DAX
     * @param job
     * @return ADAG
     * @see Job
     * @see AbstractJob
     */
    public ADAG addJob(Job job) {
        mJobs.add(job);
        return this;
    }

    /**
     * Add multiple Jobs to the DAX
     * @param jobs
     * @return ADAG
     * @see Job
     * @see AbstractJob
     */
    public ADAG addJobs(List<Job> jobs) {
        mJobs.addAll(jobs);
        return this;
    }

    /**
     * Add a DAG job to the DAX
     * @param dag the DAG to be added
     * @return ADAG
     * @see DAG
     * @see AbstractJob
     */
    public ADAG addDAG(DAG dag) {
        mJobs.add(dag);
        return this;
    }

    /**
     * Add multiple DAG jobs to the DAX
     * @param dags List of DAG jobs to be added
     * @return ADAG
     * @see DAG
     * @see AbstractJob
     */
    public ADAG addDAGs(List<DAG> dags) {
        mJobs.addAll(dags);
        return this;
    }

    /**
     * Add a DAX job to the DAX
     * @param dax DAX to be added
     * @return ADAG
     * @see DAX
     * @see AbstractJob
     */
    public ADAG addDAX(DAX dax) {
        mJobs.add(dax);
        return this;
    }

    /**
     * Add multiple DAX jobs to the DAX
     * @param daxs LIST of DAX jobs to be added
     * @return ADAG
     * @see DAX
     * @see AbstractJob
     */
    public ADAG addDAXs(List<DAX> daxs) {
        mJobs.addAll(daxs);
        return this;
    }

    /**
     * Add a parent child dependency between two jobs,dax,dag
     * @param parent String job,dax,dag id
     * @param child String job,dax,dag,id
     * @return ADAG
     *
     */
    public ADAG addDependency(String parent, String child) {
        addDependency(parent, child, null);
        return this;
    }

    /**
     * Add a parent child dependency with a dependency label
     * @param parent String job,dax,dag id
     * @param child String job,dax,dag id
     * @param label String dependency label
     * @return ADAG
     */
    public ADAG addDependency(String parent, String child, String label) {
        List<Parent> parents = mDependencies.get(child);
        if (parents == null) {
            parents = new LinkedList();
        }
        Parent p = new Parent(parent, label);
        parents.add(p);
        mDependencies.put(child, parents);
        return this;
    }

    /**
     * Generate a DAX File out of this object;
     * @param daxfile The file to write the DAX to
     */
    public void writeToFile(String daxfile) {
        try {
            mWriter = new XMLWriter(new FileWriter(daxfile));
            toXML(mWriter);
            mWriter.close();
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    /**
     * Generate a DAX representation on STDOUT.
     */
    public void writeToSTDOUT() {
        mWriter = new XMLWriter(new BufferedWriter(new OutputStreamWriter(System.out)));
        toXML(mWriter);
        mWriter.close();
    }

    /**
     * Generate a DAX representation and pipe it into the Writer
     * @param writer A Writer object
     * @param close Whether writer should be closed on return. 
     */
    public void writeToWriter(Writer writer, boolean close) {
        mWriter = new XMLWriter(writer);
        toXML(mWriter);
        if (close) {
            mWriter.close();
        }
    }

    /**
     * Generates a DAX representation.
     * @param writer
     * @
     */
    public void toXML(XMLWriter writer) {
        int indent = 0;
        writer.startElement("adag");
        writer.writeAttribute("xmlns", SCHEMA_NAMESPACE);
        writer.writeAttribute("xmlns:xsi", SCHEMA_NAMESPACE_XSI);
        writer.writeAttribute("xsi:schemaLocation", SCHEMA_NAMESPACE + " " + SCHEMA_LOCATION);
        writer.writeAttribute("version", SCHEMA_VERSION);
        writer.writeAttribute("name", mName);
        writer.writeAttribute("index", Integer.toString(mIndex));
        writer.writeAttribute("count", Integer.toString(mCount));

        //print file
        writer.writeXMLComment("Section 1: Files - Acts as a Replica Catalog (can be empty)",true);
        for (File f : mFiles) {
            f.toXML(writer, indent + 1);
        }

        //print executable
        writer.writeXMLComment("Section 2: Executables - Acts as a Transformaton Catalog (can be empty)",true);
        for (Executable e : mExecutables) {
            e.toXML(writer, indent + 1);
        }

        //print transformation
        writer.writeXMLComment("Section 3: Transformations - Aggregates executables and Files (can be empty)",true);
        for (Transformation t : mTransformations) {
            t.toXML(writer, indent + 1);
        }
        //print jobs, daxes and dags
        writer.writeXMLComment("Section 4: Job's, DAX's or Dag's - Defines a JOB or DAX or DAG (Atleast 1 required)",true);
        for (AbstractJob j : mJobs) {
            j.toXML(writer, indent + 1);
        }
        //print dependencies
        writer.writeXMLComment("Section 5: Dependencies - Parent Child relationships (can be empty)",true);

        for (String child : mDependencies.keySet()) {
            writer.startElement("child", indent + 1).writeAttribute("ref", child);
            for (Parent p : mDependencies.get(child)) {
                p.toXML(writer, indent + 2);
            }
            writer.endElement(indent + 1);
        }
        //end adag
        writer.endElement();
    }

    /**
     * Create an example DIAMOND DAX
     * @param args
     */
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("Usage: java ADAG <filename.dax>");
            System.exit(1);
        }
        Diamond().writeToFile(args[0]);

    }

    private static ADAG Diamond() {
        ADAG dax = new ADAG("test");

        File fa = new File("f.a");
        fa.addMetaData("string", "foo", "bar");
        fa.addMetaData("int", "num", "1");
        fa.addProfile("env", "FOO", "/usr/bar");
        fa.addProfile("globus", "walltime", "40");
        fa.addPhysicalFile("file:///scratch/f.a", "local");
        dax.addFile(fa);

        File fb1 = new File("f.b1");
        fb1.addMetaData("string", "foo", "bar");
        fb1.addMetaData("int", "num", "2");
        fb1.addProfile("env", "GOO", "/usr/foo");
        fb1.addProfile("globus", "walltime", "40");
        dax.addFile(fb1);

        File fb2 = new File("f.b2");
        fb2.addMetaData("string", "foo", "bar");
        fb2.addMetaData("int", "num", "3");
        fb2.addProfile("env", "BAR", "/usr/goo");
        fb2.addProfile("globus", "walltime", "40");
        dax.addFile(fb2);

        File fc1 = new File("f.c1");
        fc1.addProfile("env", "TEST", "/usr/bin/true");
        fc1.addProfile("globus", "walltime", "40");
        dax.addFile(fc1);

        File fc2 = new File("f.c2");
        fc2.addMetaData("string", "foo", "bar");
        fc2.addMetaData("int", "num", "5");
        dax.addFile(fc2);

        File fd = new File("f.d");
        dax.addFile(fd);

        Executable preprocess = new Executable("pegasus", "preproces", "1.0");
        preprocess.setArchitecture(Executable.ARCH.x86).setOS(Executable.OS.LINUX);
        preprocess.setInstalled(false);
        preprocess.addPhysicaFile(new PFN("file:///opt/pegasus/default/bin/keg"));
        preprocess.addProfile(Profile.NAMESPACE.globus, "walltime", "120");
        preprocess.addMetaData("string", "project", "pegasus");

        Executable findrange = new Executable("pegasus", "findrange", "1.0");
        findrange.setArchitecture(Executable.ARCH.x86).setOS(Executable.OS.LINUX);
        findrange.unsetInstalled();
        findrange.addPhysicaFile(new PFN("http://pegasus.isi.edu/code/bin/keg"));
        findrange.addProfile(Profile.NAMESPACE.globus, "walltime", "120");
        findrange.addMetaData("string", "project", "pegasus");


        Executable analyze = new Executable("pegasus", "analyze", "1.0");
        analyze.setArchitecture(Executable.ARCH.x86).setOS(Executable.OS.LINUX);
        analyze.unsetInstalled();
        analyze.addPhysicaFile(new PFN("gsiftp://localhost/opt/pegasus/default/bin/keg"));
        analyze.addProfile(Profile.NAMESPACE.globus, "walltime", "120");
        analyze.addMetaData("string", "project", "pegasus");

        dax.addExecutable(preprocess).addExecutable(findrange).addExecutable(analyze);

        Transformation diamond = new Transformation("pegasus", "diamond", "1.0");
        diamond.uses(preprocess).uses(findrange).uses(analyze);
        diamond.uses(new File("config", File.LINK.INPUT));

        dax.addTransformation(diamond);


        Job j1 = new Job("j1", "pegasus", "preprocess", "1.0", "j1");
        j1.addArgument("-a preprocess -T 60 -i ").addArgument(fa);
        j1.addArgument("-o ").addArgument(fb1).addArgument(fb2);
        j1.uses(fa, File.LINK.INPUT);
        j1.uses(fb1, File.LINK.OUTPUT);
        j1.uses(new File("f.b2"), File.LINK.OUTPUT);
        j1.addProfile(Profile.NAMESPACE.dagman, "pre", "20");
        dax.addJob(j1);

        DAG j2 = new DAG("j2", "findrange.dag", "j2");
        j2.uses(new File("f.b1"), File.LINK.INPUT);
        j2.uses(new File("f.c1"), File.LINK.OUTPUT);
        j2.addProfile(Profile.NAMESPACE.dagman, "pre", "20");
        j2.addProfile("condor", "universe", "vanilla");
        dax.addDAG(j2);

        DAX j3 = new DAX("j3", "findrange.dax", "j3");
        j3.addArgument("--site ").addArgument("local");
        j3.uses(new File("f.b2"), File.LINK.INPUT);
        j3.uses(new File("f.c2"), File.LINK.OUTPUT);
        j3.addInvoke(Invoke.WHEN.start, "/bin/notify -m START gmehta@isi.edu");
        j3.addInvoke(Invoke.WHEN.at_end, "/bin/notify -m END gmehta@isi.edu");
        j3.addProfile("ENV", "HAHA", "YADAYADAYADA");
        dax.addDAX(j3);

        Job j4 = new Job("j4", "pegasus", "analyze", "");
        j4.addArgument("-a analyze -T 60 -i ").addArgument(fc1);
        j4.addArgument(" ").addArgument(fc2);
        j4.addArgument("-o ").addArgument(fd);
        j4.uses(fc1, File.LINK.INPUT);
        j4.uses(fc2, File.LINK.INPUT);
        j4.uses(fd, File.LINK.OUTPUT);
        dax.addJob(j4);

        dax.addDependency("j1", "j2", "1-2");
        dax.addDependency("j1", "j3", "1-3");
        dax.addDependency("j2", "j4");
        dax.addDependency("j3", "j4");
        return dax;
    }
}
