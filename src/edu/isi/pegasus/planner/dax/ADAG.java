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
import java.io.FileWriter;
import java.io.IOException;

import edu.isi.pegasus.common.util.XMLWriter;

/**
 *
 * @author gmehta
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
    private String mName;
    private int mIndex;
    private int mCount;
    private int mJobCount;
    private int mFileCount;
    private int mChildCount;
    List<AbstractJob> mJobs;
    List<Transformation> mTransformations;
    List<Executable> mExecutables;
    List<File> mFiles;
    private XMLWriter mWriter;
    private Map<String, List<Parent>> mDependencies;

    public ADAG(String name) {
        this(name, 0, 1);
    }

    public ADAG(String name, int index, int count) {
        mName = name;
        mIndex = index;
        mCount = count;
        mJobs = new LinkedList<AbstractJob>();
        mTransformations = new LinkedList<Transformation>();
        mExecutables = new LinkedList<Executable>();
        mFiles = new LinkedList<File>();
        mDependencies = new HashMap();
    }

    public ADAG addFile(File file) {
        mFiles.add(file);
        return this;
    }

    public ADAG addFiles(List<File> files) {
        mFiles.addAll(files);
        return this;
    }

    public ADAG addExecutable(Executable executable) {
        mExecutables.add(executable);
        return this;
    }

    public ADAG addExecutables(List<Executable> executables) {
        mExecutables.addAll(executables);
        return this;
    }

    public ADAG addTransformation(Transformation transformation) {
        mTransformations.add(transformation);
        return this;
    }

    public ADAG addTransformations(List<Transformation> transformations) {
        mTransformations.addAll(transformations);
        return this;
    }

    public ADAG addJob(Job job) {
        mJobs.add(job);
        return this;
    }

    public ADAG addJobs(List<Job> jobs) {
        mJobs.addAll(jobs);
        return this;
    }

    public ADAG addDAG(DAG dag) {
        mJobs.add(dag);
        return this;
    }

    public ADAG addDAGs(List<DAG> dags) {
        mJobs.addAll(dags);
        return this;
    }

    public ADAG addDAX(DAX dax) {
        mJobs.add(dax);
        return this;
    }

    public ADAG addDAXs(List<DAX> daxs) {
        mJobs.addAll(daxs);
        return this;
    }

    public ADAG addDependency(String parent, String child) {
        addDependency(parent, child, null);
        return this;
    }

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

    public void writeToFile(String daxfile) {
        try {
            mWriter = new XMLWriter(new FileWriter(daxfile));
            toXML(mWriter);
            mWriter.close();
//            Serializer serializer = new Persister();
//            serializer.write(this, new java.io.File(daxfile));
        } catch (IOException ioe) {
            ioe.printStackTrace();
        }
    }

    public void toXML(XMLWriter writer) {
        int indent = 0;
        writer.startElement("adag");
        writer.writeAttribute("xmlns", SCHEMA_NAMESPACE);
        writer.writeAttribute("xmlns:xsi", SCHEMA_NAMESPACE_XSI);
        writer.writeAttribute("xsi:schemaLocation", SCHEMA_LOCATION);
        writer.writeAttribute("version", SCHEMA_VERSION);
        writer.writeAttribute("name", mName);
        writer.writeAttribute("index", Integer.toString(mIndex));
        writer.writeAttribute("count", Integer.toString(mCount));

        if (mJobCount > 0) {
            writer.writeAttribute("jobcount", Integer.toString(mJobCount));
        }
        if (mFileCount > 0) {
            writer.writeAttribute("filecount", Integer.toString(mFileCount));
        }
        if (mChildCount > 0) {
            writer.writeAttribute("childcount", Integer.toString(mChildCount));
        }

        //print file
        writer.writeXMLComment("Section 1: Files - Acts as a Replica Catalog (can be empty)");
        for (File f : mFiles) {
            f.toXML(writer, indent + 1);
        }

        //print executable
        writer.writeXMLComment("Section 2: Executables - Acts as a Transformaton Catalog (can be empty)");
        for (Executable e : mExecutables) {
            e.toXML(writer, indent + 1);
        }

        //print transformation
        writer.writeXMLComment("Section 3: Transformations - Aggregates executables and Files (can be empty)");
        for (Transformation t : mTransformations) {
            t.toXML(writer, indent + 1);
        }
        //print jobs, daxes and dags
        writer.writeXMLComment("Section 4: Job's, DAX's or Dag's - Defines a JOB or DAX or DAG (Atleast 1 required)");
        for (AbstractJob j : mJobs) {
            j.toXML(writer, indent + 1);
        }
        //print dependencies
        writer.writeXMLComment("Section 5: Dependencies - Parent Child relationships (can be empty)");

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

    public static void main(String[] args) {
        Diamond().writeToFile("/tmp/test.dax");

    }

    public static ADAG Diamond() {
        ADAG dax = new ADAG("test");

        File fa = new File("f.a", File.LINK.INPUT);
        fa.addMetaData("string", "foo", "bar");
        fa.addMetaData("int", "num", "1");
        fa.addProfile("env", "PATH", "/usr/bin");
        fa.addProfile("globus", "walltime", "40");
        fa.addPhysicalFile("file:///scratch/f.a", "local");
        dax.addFile(fa);

        File fb1 = new File("f.b1", File.LINK.INOUT);
        fb1.addMetaData("string", "foo", "bar");
        fb1.addMetaData("int", "num", "1");
        fb1.addProfile("env", "PATH", "/usr/bin");
        fb1.addProfile("globus", "walltime", "40");
        dax.addFile(fb1);

        File fb2 = new File("f.b2", File.LINK.INOUT);
        fb2.addMetaData("string", "foo", "bar");
        fb2.addMetaData("int", "num", "1");
        fb2.addProfile("env", "PATH", "/usr/bin");
        fb2.addProfile("globus", "walltime", "40");
        dax.addFile(fb2);

        File fc1 = new File("f.c1", File.LINK.INOUT);
        fc1.addProfile("env", "PATH", "/usr/bin");
        fc1.addProfile("globus", "walltime", "40");
        dax.addFile(fc1);

        File fc2 = new File("f.c2", File.LINK.INOUT);
        fc2.addMetaData("string", "foo", "bar");
        fc2.addMetaData("int", "num", "1");
        dax.addFile(fc2);

        File fd = new File("f.d", File.LINK.OUTPUT);
        dax.addFile(fd);

        Job j1 = new Job("j1", "pegasus", "preprocess", "1.0", "j1");
        j1.addArgument("-a preprocess -i ").addArgument(fa);
        j1.addArgument("-o ").addArgument(fb1).addArgument(fb2);
        j1.addUses(new File("f.a", File.LINK.INPUT));
        j1.addUses(new File("f.b1", File.LINK.OUTPUT));
        j1.addUses(new File("f.b2", File.LINK.OUTPUT));
        j1.addProfile(Profile.NAMESPACE.dagman, "pre", "20");
        j1.addProfile("condor", "universe", "vanilla");
        dax.addJob(j1);

        DAG j2 = new DAG("j2", "findrange.dag", "j2");
        j2.addUses(new File("f.b1", File.LINK.INPUT));
        j2.addUses(new File("f.c1", File.LINK.OUTPUT));
        j2.addProfile(Profile.NAMESPACE.dagman, "pre", "20");
        j2.addProfile("condor", "universe", "vanilla");
        dax.addDAG(j2);

        DAX j3 = new DAX("j3", "findrange.dax", "j3");
        j3.addArgument("--site ").addArgument("local");
        j3.addUses(new File("f.b2", File.LINK.INPUT));
        j3.addUses(new File("f.c2", File.LINK.OUTPUT));
        j3.addInvoke(Invoke.WHEN.start, "/bin/notify -m START gmehta@isi.edu");
        j3.addInvoke(Invoke.WHEN.at_end, "/bin/notify -m END gmehta@isi.edu");
        j3.addProfile("condor", "universe", "vanilla");
        dax.addDAX(j3);

        Job j4 = new Job("j4", "pegasus", "analyze", "");
        j4.addArgument("-a analyze -i ").addArgument(fc1).addArgument(fc2);
        j4.addArgument("-o ").addArgument(fd);
        fc1.mLink = File.LINK.INPUT;
        j4.addUses(fc1);
        j4.addUses(fc2);
        j4.addUses(fd);
        dax.addJob(j4);

        dax.addDependency("j1", "j2", "1-2");
        dax.addDependency("j1", "j3", "1-3");
        dax.addDependency("j2", "j4");
        dax.addDependency("j3", "j4");
        return dax;
    }
}
