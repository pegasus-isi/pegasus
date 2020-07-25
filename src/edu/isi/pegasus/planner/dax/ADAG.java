/**
 * Copyright 2007-2012 University Of Southern California
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
package edu.isi.pegasus.planner.dax;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.Currently;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.common.util.XMLWriter;
import edu.isi.pegasus.planner.catalog.replica.classes.ReplicaStore;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.common.PegasusJsonSerializer;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.dax.Invoke.WHEN;
import edu.isi.pegasus.planner.namespace.Metadata;
import edu.isi.pegasus.planner.parser.dax.Callback;
import edu.isi.pegasus.planner.parser.dax.DAX2CDAG;
import edu.isi.pegasus.planner.parser.dax.DAXParser5;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.log4j.Level;

/**
 * <pre>
 * <b>This class provides the Java API to create Abstract Workflow files.</b>
 * Starting 5.0 Release, this class by default writes out YAML formatted abstract
 * workflow file. The information included in the generated abstract workflow is a
 * subset of the Pegasus Workflow YAML schema that is available at
 * <http://pegasus.isi.edu/schema/wf-5.0.yml">http://pegasus.isi.edu/schema/wf-5.0.yml</a>
 *
 * In particular, it is missing options to create a Site Catalog, and associate
 * Containers with user executables. If you need this information, we recommend you
 * use the python Pegasus workflow API.
 *
 *
 * The Abstract Workflow consists of 6 parts the first 4 are optional and the last is optional.
 * </pre> <ol> <li><b>file:</b>Used as "In Abstract Workflow" Replica Catalog
 * (Optional)</li><br> <li><b>executable:</b> Used as "In Abstract Workflow" Transformation
 * Catalog (Optional)</li><br> <li><b>transformation:</b> Used to describe
 * compound executables. i.e. Executable depending on other executables
 * (Optional)</li><br> <li><b>job|dax|dag:</b> Used to describe a single job or
 * sub dax or sub dax. Atleast 1 required.</li><br> <li><b>child:</b> The
 * dependency section to describe dependencies between job|dax|dag elements.
 * (Optional)</li><br> </ol> <center><img
 * src="http://pegasus.isi.edu/wms/docs/schemas/dax-3.6/dax-3.6_p1.png"/></center>
 * <pre>
 * To generate an example DIAMOND Abstract Workflow run the ADAG Class as shown below
 * <b>java ADAG filename</b>
 * <b>NOTE: This is an illustrative example only. Please see examples directory for a working example</b>
 *
 * Here is sample java code that illustrates how to use the Java Abstract Workflow API
 * <pre>
 *      java.io.File cwdFile = new java.io.File (".");
 * String cwd = cwdFile.getCanonicalPath();
 *
 * String pegasusHome = "/usr";
 * String site = "TestCluster";
 *
 * ADAG dax = new ADAG("diamond");
 * dax.addNotification(Invoke.WHEN.start,"/pegasus/libexec/notification/email -t notify@example.com");
 * dax.addNotification(Invoke.WHEN.at_end,"/pegasus/libexec/notification/email -t notify@example.com");
 * dax.addMetadata( "name", "diamond");
 * dax.addMetadata( "createdBy", "Karan Vahi");
 *
 * File fa = new File("f.a");
 * fa.addPhysicalFile("file://" + cwd + "/f.a", "local");
 * fa.addMetaData( "size", "1024" );
 * dax.addFile(fa);
 *
 * File fb1 = new File("f.b1");
 * File fb2 = new File("f.b2");
 * File fc1 = new File("f.c1");
 * File fc2 = new File("f.c2");
 * File fd = new File("f.d");
 * fd.setRegister(true);
 *
 * Executable preprocess = new Executable("pegasus", "preprocess", "4.0");
 * preprocess.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
 * preprocess.setInstalled(true);
 * preprocess.addMetaData( "size", "2048" );
 * preprocess.addPhysicalFile("file://" + pegasus_location + "/bin/keg", site_handle);
 *
 * Executable findrange = new Executable("pegasus", "findrange", "4.0");
 * findrange.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
 * findrange.setInstalled(true);
 * findrange.addPhysicalFile("file://" + pegasus_location + "/bin/keg", site_handle);
 *
 * Executable analyze = new Executable("pegasus", "analyze", "4.0");
 * analyze.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
 * analyze.setInstalled(true);
 * analyze.addPhysicalFile("file://" + pegasus_location + "/bin/keg", site_handle);
 *
 * dax.addExecutable(preprocess).addExecutable(findrange).addExecutable(analyze);
 *
 * // Add a preprocess job
 * Job j1 = new Job("j1", "pegasus", "preprocess", "4.0");
 * j1.addArgument("-a preprocess -T 60 -i ").addArgument(fa);
 * j1.addArgument("-o ").addArgument(fb1);
 * j1.addArgument(" ").addArgument(fb2);
 * j1.addMetadata( "time", "60" );
 * j1.uses(fa, File.LINK.INPUT);
 * j1.uses(fb1, File.LINK.OUTPUT);
 * j1.uses(fb2, File.LINK.OUTPUT);
 * j1.addNotification(Invoke.WHEN.start,"/pegasus/libexec/notification/email -t notify@example.com");
 * j1.addNotification(Invoke.WHEN.at_end,"/pegasus/libexec/notification/email -t notify@example.com");
 * dax.addJob(j1);
 *
 * // Add left Findrange job
 * Job j2 = new Job("j2", "pegasus", "findrange", "4.0");
 * j2.addArgument("-a findrange -T 60 -i ").addArgument(fb1);
 * j2.addArgument("-o ").addArgument(fc1);
 * j2.addMetadata( "time", "60" );
 * j2.uses(fb1, File.LINK.INPUT);
 * j2.uses(fc1, File.LINK.OUTPUT);
 * j2.addNotification(Invoke.WHEN.start,"/pegasus/libexec/notification/email -t notify@example.com");
 * j2.addNotification(Invoke.WHEN.at_end,"/pegasus/libexec/notification/email -t notify@example.com");
 * dax.addJob(j2);
 *
 * // Add right Findrange job
 * Job j3 = new Job("j3", "pegasus", "findrange", "4.0");
 * j3.addArgument("-a findrange -T 60 -i ").addArgument(fb2);
 * j3.addArgument("-o ").addArgument(fc2);
 * j3.addMetadata( "time", "60" );
 * j3.uses(fb2, File.LINK.INPUT);
 * j3.uses(fc2, File.LINK.OUTPUT);
 * j3.addNotification(Invoke.WHEN.start,"/pegasus/libexec/notification/email -t notify@example.com");
 * j3.addNotification(Invoke.WHEN.at_end,"/pegasus/libexec/notification/email -t notify@example.com");
 * dax.addJob(j3);
 *
 * // Add analyze job
 * Job j4 = new Job("j4", "pegasus", "analyze", "4.0");
 * j4.addArgument("-a analyze -T 60 -i ").addArgument(fc1);
 * j4.addArgument(" ").addArgument(fc2);
 * j4.addArgument("-o ").addArgument(fd);
 * j4.addMetadata( "time", "60" );
 * j4.uses(fc1, File.LINK.INPUT);
 * j4.uses(fc2, File.LINK.INPUT);
 * j4.uses(fd, File.LINK.OUTPUT);
 * j4.addNotification(Invoke.WHEN.start,"/pegasus/libexec/notification/email -t notify@example.com");
 * j4.addNotification(Invoke.WHEN.at_end,"/pegasus/libexec/notification/email -t notify@example.com");
 * dax.addJob(j4);
 *
 * dax.addDependency("j1", "j2");
 * dax.addDependency("j1", "j3");
 * dax.addDependency("j2", "j4");
 * dax.addDependency("j3", "j4");
 * dax.writeToSTDOUT();
 * </pre>
 *
 * @author Gaurang Mehta gmehta at isi dot edu
 * @author Karan Vahi
 * @author Ryan Tanaka
 * @version $Revision$
 */
@JsonSerialize(using = ADAG.JsonSerializer.class)
public class ADAG {

    /** The "official" namespace URI of the site catalog schema. */
    public static final String SCHEMA_NAMESPACE = "https://pegasus.isi.edu/schema/DAX";
    /** XSI SCHEMA NAMESPACE */
    public static final String SCHEMA_NAMESPACE_XSI = "http://www.w3.org/2001/XMLSchema-instance";
    /** The "not-so-official" location URL of the DAX schema definition. */
    public static final String SCHEMA_LOCATION = "https://pegasus.isi.edu/schema/dax-3.6.xsd";
    /** The version to report. */
    public static final String SCHEMA_VERSION = "3.6";

    public static final String PEGASUS_VENDOR_EXTENSION_KEY = "x-pegasus";

    /**
     * Returns pegasus vendor extensions that we encode in each workflow
     *
     * @return
     */
    private Map<String, String> getPegasusVendorExtensions() {
        Map m = new HashMap();
        m.put("createdBy", System.getProperty("user.name"));
        m.put("apiLang", "java");
        m.put("createdOn", Currently.iso8601(true));
        return m;
    }

    /** Enum to indicate how the generated dax file should be formatted */
    public static enum FORMAT {
        yaml,
        xml
    };

    /** The default format to use for writing out the Abstract Workflow. */
    public static FORMAT DEFAULT_FORMAT = FORMAT.yaml;

    /** The type of Abstract Workflow API generated */
    private static final String WF_API_KEY = "java";

    /** The Name / Label of the DAX */
    private String mName;
    /** The Index of the dax object. I out of N */
    private int mIndex;
    /** The Count of the number of dax objects : N */
    private int mCount;

    /**
     * The List of Job,DAX and DAG objects
     *
     * @see DAG
     * @see DAX
     * @see Job
     * @see AbstractJob
     */
    private Map<String, AbstractJob> mJobs;

    private List<Job> mLJobs;
    private List<DAG> mLDAGs;
    private List<DAX> mLDAXs;
    /**
     * The List of Transformation objects
     *
     * @see Transformation
     */
    private Set<Transformation> mTransformations;
    /**
     * The list of Executable objects
     *
     * @see Executable
     */
    private Set<Executable> mExecutables;
    /**
     * The list of edu.isi.pegasus.planner.dax.File objects
     *
     * @see File
     */
    private List<File> mFiles;

    /**
     * Map of Dependencies between Job,DAX,DAG objects. Map key is a string that holds the child
     * element reference, the value is a List of Parent objects
     *
     * @see Parent
     */
    private Map<String, Set<Edge>> mDependencies;
    /** List of Notification objects */
    private List<Invoke> mInvokes;

    /** The metadata attributes associated with the whole workflow. */
    private Set<MetaData> mMetaDataAttributes;

    /** Handle the XML writer */
    private XMLWriter mWriter;

    private LogManager mLogger;

    /**
     * The Simple constructor for the DAX object
     *
     * @param name DAX LABEL
     */
    public ADAG(String name) {
        this(name, 0, 1);
    }

    /**
     * DAX Constructor
     *
     * @param name DAX Label
     * @param index Index of DAX out of N DAX's
     * @param count Number of DAXS in a group
     */
    public ADAG(String name, int index, int count) {
        // initialize everything
        mName = name;
        mIndex = index;
        mCount = count;
        mJobs = new LinkedHashMap<String, AbstractJob>();
        mLJobs = new LinkedList<Job>();
        mLDAGs = new LinkedList<DAG>();
        mLDAXs = new LinkedList<DAX>();
        mTransformations = new LinkedHashSet<Transformation>();
        mExecutables = new LinkedHashSet<Executable>();
        mMetaDataAttributes = new LinkedHashSet<MetaData>();
        mMetaDataAttributes.add(new MetaData(Metadata.WF_API_KEY, WF_API_KEY));
        mFiles = new LinkedList<File>();
        mInvokes = new LinkedList<Invoke>();
        mDependencies = new LinkedHashMap<String, Set<Edge>>();
        mLogger = LogManagerFactory.loadSingletonInstance();
        mLogger.logEventStart(
                "event.dax.generate", "pegasus.version", Version.instance().toString());
    }

    /**
     * Return the name/label of the dax
     *
     * @return
     */
    public String getName() {
        return mName;
    }

    /* Return the index of the dax
     * @return int
     */
    public int getIndex() {
        return mIndex;
    }

    /**
     * Returns the total count of the dax collection. (legacy)
     *
     * @return int
     */
    public int getCount() {
        return mCount;
    }

    /**
     * Add a Notification for this Workflow
     *
     * @param when
     * @param what
     * @return ADAG
     */
    public ADAG addInvoke(Invoke.WHEN when, String what) {
        Invoke i = new Invoke(when, what);
        mInvokes.add(i);
        return this;
    }

    /**
     * Add a Notification for this Workflow
     *
     * @param when
     * @param what
     * @return ADAG
     */
    public ADAG addNotification(Invoke.WHEN when, String what) {
        return addInvoke(when, what);
    }

    /**
     * Add a Notification for this Workflow
     *
     * @param invoke
     * @return ADAG
     */
    public ADAG addInvoke(Invoke invoke) {
        mInvokes.add(invoke.clone());
        return this;
    }

    /**
     * Add a Notification for this Workflow
     *
     * @param invoke
     * @return ADAG
     */
    public ADAG addNotification(Invoke invoke) {
        return addInvoke(invoke);
    }

    /**
     * Add a List of Notifications for this Workflow
     *
     * @param invokes
     * @return ADAG
     */
    public ADAG addInvokes(List<Invoke> invokes) {
        for (Invoke invoke : invokes) {
            this.addInvoke(invoke);
        }
        return this;
    }

    /**
     * Add a List of Notifications for this Workflow
     *
     * @param invokes
     * @return ADAG
     */
    public ADAG addNotifications(List<Invoke> invokes) {
        return addInvokes(invokes);
    }

    /**
     * Returns a list of Invoke objects associated with the workflow
     *
     * @return
     */
    public List<Invoke> getInvoke() {
        return mInvokes;
    }

    /**
     * Returns a list of Invoke objects associated with the workflow. Same as getInvoke()
     *
     * @return
     */
    public List<Invoke> getNotification() {
        return getInvoke();
    }

    /**
     * Adds metadata to the workflow
     *
     * @param key key name for metadata
     * @param value value
     * @return
     */
    public ADAG addMetaData(String key, String value) {
        this.mMetaDataAttributes.add(new MetaData(key, value));
        return this;
    }

    /**
     * Returns the metadata associated for a key if exists, else null
     *
     * @param key
     * @return
     */
    public String getMetaData(String key) {
        return this.mMetaDataAttributes.contains(key)
                ? ((MetaData) mMetaDataAttributes).getValue()
                : null;
    }

    /**
     * Add a RC File object to the top of the DAX.
     *
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
     *
     * @param files List<File> List of file objects to be added to the RC Section
     * @return ADAG
     * @see File
     */
    public ADAG addFiles(List<File> files) {
        mFiles.addAll(files);
        return this;
    }

    /**
     * Returns a list of File objects defined as the inDax Replica Catalog
     *
     * @return
     */
    public List<File> getFiles() {
        return mFiles;
    }

    /**
     * Add Executable to the DAX
     *
     * @param executable Executable to be added
     * @return ADAG
     * @see Executable
     */
    public ADAG addExecutable(Executable executable) {
        if (executable != null) {
            if (!mExecutables.contains(executable)) {
                mExecutables.add(executable);
            } else {
                throw new RuntimeException(
                        "Error: Executable "
                                + executable.toString()
                                + " already exists in the DAX.\n");
            }
        } else {
            throw new RuntimeException("Error: The executable passed is null\n");
        }
        return this;
    }

    /**
     * Add Multiple Executable objects to the DAX
     *
     * @param executables List of Executable objects to be added
     * @return ADAG
     * @see Executable
     */
    public ADAG addExecutables(List<Executable> executables) {
        if (executables != null && !executables.isEmpty()) {
            for (Executable executable : executables) {
                addExecutable(executable);
            }
        } else {
            throw new RuntimeException(
                    "Error: The executables list to be added is either null or empty\n");
        }
        return this;
    }

    /**
     * Returns a set of Executable Objects stored as part of the inDAX Transformation Catalog;
     *
     * @return
     */
    public Set<Executable> getExecutables() {
        return mExecutables;
    }

    /**
     * Checks if a given executable exists in the DAX based Transformation Catalog
     *
     * @param executable
     * @return boolean
     */
    public boolean containsExecutable(Executable executable) {
        return mExecutables.contains(executable);
    }

    /**
     * Add Transformation to the DAX
     *
     * @param transformation Transformation object to be added
     * @return ADAG
     * @see Transformation
     */
    public ADAG addTransformation(Transformation transformation) {
        if (transformation != null) {
            if (!mTransformations.contains(transformation)) {
                mTransformations.add(transformation);
            } else {
                throw new RuntimeException(
                        "Error: Transformation "
                                + transformation.toString()
                                + " already exists in the DAX.\n");
            }
        } else {
            throw new RuntimeException("Transformation provided is null\n");
        }
        return this;
    }

    /**
     * Add Multiple Transformation to the DAX
     *
     * @param transformations List of Transformation objects
     * @return ADAG
     * @see Transformation
     */
    public ADAG addTransformations(List<Transformation> transformations) {
        if (transformations != null && !transformations.isEmpty()) {

            for (Transformation transformation : transformations) {
                addTransformation(transformation);
            }
        } else {
            throw new RuntimeException("List of transformations provided is null");
        }
        return this;
    }

    /**
     * Checks if a given Transformation exists in the DAX based Transformation Catalog
     *
     * @param transformation Transformation
     * @return boolean
     */
    public boolean containsTransformation(Transformation transformation) {
        return mTransformations.contains(transformation);
    }

    /**
     * Returns a set of Transformation Objects (complex executables) stored in the DAX based
     * Transformation Catalog
     *
     * @return
     */
    public Set<Transformation> getTransformations() {
        return mTransformations;
    }

    /**
     * Add AbstractJob to the DAX
     *
     * @param ajob AbstractJob
     * @return ADAG
     * @see Job
     * @see DAG
     * @see DAX
     * @see AbstractJob
     */
    private ADAG addAbstractJob(AbstractJob ajob) {
        if (!mJobs.containsKey(ajob.mId)) {
            mJobs.put(ajob.mId, ajob);
            if (ajob.isDAG()) {
                mLDAGs.add((DAG) ajob);
            } else if (ajob.isDAX()) {
                mLDAXs.add((DAX) ajob);
            } else if (ajob.isJob()) {
                mLJobs.add((Job) ajob);
            }
        } else {
            throw new RuntimeException(
                    "Job of type"
                            + ajob.getClass().getSimpleName()
                            + " with jobid "
                            + ajob.mId
                            + " already exists in the DAX");
        }
        return this;
    }

    /**
     * Add AbstractJobs to the DAX
     *
     * @param ajobs AbstractJob
     * @return ADAG
     * @see Job
     * @see DAG
     * @see DAX
     * @see AbstractJob
     */
    private ADAG addAbstractJobs(List<AbstractJob> ajobs) {
        for (AbstractJob ajob : ajobs) {
            addAbstractJob(ajob);
        }
        return this;
    }

    /**
     * Returns an abstract Job with id ajobid if present otherwise null.
     *
     * @param ajobid
     * @return
     */
    private AbstractJob getAbstractJob(String ajobid) {
        if (ajobid != null) {
            AbstractJob j = mJobs.get(ajobid);
            if (j != null) {
                return j;
            } else {
                mLogger.log(
                        "No Job/DAX/DAG found with id " + ajobid, LogManager.ERROR_MESSAGE_LEVEL);
            }
        }
        return null;
    }

    /**
     * Check if an abstractjob exists in the DAX
     *
     * @param ajob
     * @return
     */
    private boolean containsAbstractJob(AbstractJob ajob) {
        return containsAbstractJobId(ajob.mId);
    }

    /**
     * Check if a jobid exists in the DAX
     *
     * @param ajobid
     * @return
     */
    private boolean containsAbstractJobId(String ajobid) {
        return mJobs.containsKey(ajobid);
    }

    /**
     * Add Job to the DAX
     *
     * @param job
     * @return ADAG
     * @see Job
     * @see AbstractJob
     */
    public ADAG addJob(Job job) {
        return addAbstractJob(job);
    }

    /**
     * Add multiple Jobs to the DAX
     *
     * @param jobs
     * @return ADAG
     * @see Job
     * @see AbstractJob
     */
    public ADAG addJobs(List<Job> jobs) {
        for (Job job : jobs) {
            addJob(job);
        }
        return this;
    }

    /**
     * Check if a job exists in the DAX
     *
     * @param job
     * @return
     */
    public boolean containsJob(Job job) {
        return containsAbstractJob(job);
    }

    /**
     * Check if a jobid exists in the DAX
     *
     * @param jobid
     * @return
     */
    public boolean containsJobId(String jobid) {
        return containsAbstractJobId(jobid);
    }

    /**
     * Returns a Job object with id jobid if present otherwise null.
     *
     * @param jobid
     * @return
     */
    public Job getJob(String jobid) {
        AbstractJob j = getAbstractJob(jobid);
        if (j != null) {
            if (j.isJob()) {
                return (Job) j;
            } else {
                mLogger.log(
                        "Returned object is not of type Job, but " + j.getClass().getSimpleName(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            }
        }
        return null;
    }

    /**
     * Get a list of all the DAG jobs.
     *
     * @return
     */
    public List<Job> getJobs() {
        return mLJobs;
    }

    /**
     * Get a list of all the DAX jobs.
     *
     * @return
     */
    public List<DAX> getDAXs() {
        return mLDAXs;
    }

    /**
     * Returns a DAX object with id daxid if present otherwise null.
     *
     * @param daxid
     * @return
     */
    public DAX getDAX(String daxid) {

        AbstractJob j = getAbstractJob(daxid);
        if (j != null) {
            if (j.isDAX()) {
                return (DAX) j;
            } else {
                mLogger.log(
                        "Return object is not of type DAX, but " + j.getClass().getSimpleName(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            }
        }
        return null;
    }

    /**
     * Get a list of all the DAG jobs.
     *
     * @return
     */
    public List<DAG> getDAGs() {
        return mLDAGs;
    }

    /**
     * Returns a DAG object with id dagid if present otherwise null.
     *
     * @param dagid
     * @return
     */
    public DAG getDAG(String dagid) {
        AbstractJob j = getAbstractJob(dagid);
        if (j != null) {
            if (j.isDAG()) {
                return (DAG) j;
            } else {
                mLogger.log(
                        "Return object is not of type DAG, but " + j.getClass().getSimpleName(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            }
        }
        return null;
    }

    /**
     * Add a DAG job to the DAX
     *
     * @param dag the DAG to be added
     * @return ADAG
     * @see DAG
     * @see AbstractJob
     */
    public ADAG addDAG(DAG dag) {
        return addAbstractJob(dag);
    }

    /**
     * Add multiple DAG jobs to the DAX
     *
     * @param dags List of DAG jobs to be added
     * @return ADAG
     * @see DAG
     * @see AbstractJob
     */
    public ADAG addDAGs(List<DAG> dags) {
        for (DAG dag : dags) {
            addDAG(dag);
        }
        return this;
    }

    /**
     * Check if a DAG job exists in the DAX
     *
     * @param dag
     * @return
     */
    public boolean containsDAG(DAG dag) {
        return containsAbstractJob(dag);
    }

    /**
     * Check if a DAG job id exists in the DAX
     *
     * @param dagid
     * @return
     */
    public boolean containsDAGId(String dagid) {
        return containsAbstractJobId(dagid);
    }

    /**
     * Add a DAX job to the DAX
     *
     * @param dax DAX to be added
     * @return ADAG
     * @see DAX
     * @see AbstractJob
     */
    public ADAG addDAX(DAX dax) {
        return addAbstractJob(dax);
    }

    /**
     * Add multiple DAX jobs to the DAX
     *
     * @param daxs LIST of DAX jobs to be added
     * @return ADAG
     * @see DAX
     * @see AbstractJob
     */
    public ADAG addDAXs(List<DAX> daxs) {
        for (DAX dax : daxs) {
            addDAX(dax);
        }
        return this;
    }

    /**
     * Check if a DAX job exists in the DAX
     *
     * @param dax
     * @return
     */
    public boolean containsDAX(DAX dax) {
        return containsAbstractJob(dax);
    }

    /**
     * Check if a DAX job id exists in the DAX
     *
     * @param daxid
     * @return
     */
    public boolean containsDAXId(String daxid) {
        return containsAbstractJobId(daxid);
    }

    /**
     * Add a parent child dependency between two jobs,dax,dag
     *
     * @param parent String job,dax,dag id
     * @param child String job,dax,dag,id
     * @return ADAG
     */
    public ADAG addDependency(String parent, String child) {
        addDependency(parent, child, null);
        return this;
    }

    /**
     * Add a parent child dependency between two jobs,dax,dag
     *
     * @param parent Job|DAX|DAG object
     * @param child Job|DAX|DAG object
     * @return
     */
    public ADAG addDependency(AbstractJob parent, AbstractJob child) {
        addDependency(parent.getId(), child.getId(), null);
        return this;
    }

    /**
     * Add a parent child dependency with a dependency label
     *
     * @param parent String job,dax,dag id
     * @param child String job,dax,dag id
     * @param label String dependency label
     * @return ADAG
     */
    public ADAG addDependency(String parent, String child, String label) {
        if (containsAbstractJobId(parent) && containsAbstractJobId(child)) {
            Set<Edge> edges = mDependencies.get(child);
            if (edges == null) {
                edges = new LinkedHashSet<Edge>();
            }
            Edge e = new Edge(parent, child, label);
            edges.add(e);
            mDependencies.put(child, edges);
        } else {
            throw new RuntimeException(
                    "Either Job with id "
                            + parent
                            + " or "
                            + child
                            + "is not added to the DAX.\n"
                            + "Please add the jobs first to the dax and then add the dependencies between them\n");
        }
        return this;
    }

    /**
     * Returns a list of Edge objects for a child job/dax/dag id. Returns an empty set if the child
     * does not have any parents Returns null if the child is not a valid job/dax/dag id
     *
     * @param child
     * @return
     */
    public Set<Edge> getEdges(String child) {
        if (child != null && mJobs.containsKey(child)) {
            return mDependencies.containsKey(child)
                    ? mDependencies.get(child)
                    : new LinkedHashSet<Edge>();
        }
        return null;
    }

    /**
     * Returns a Set of all the Edge objects for the DAX. Returns empty if no dependencies.
     *
     * @param child
     * @return
     */
    public Set<Edge> getEdges() {
        Set<Edge> edges = new LinkedHashSet<Edge>();
        for (Set<Edge> s : mDependencies.values()) {
            edges.addAll(s);
        }
        return edges;
    }
    /**
     * Add a parent child dependency with a dependency label
     *
     * @param parent Job|DAX|DAG object
     * @param child Job|DAX|DAG object
     * @param label String label for annotation
     * @return ADAG
     */
    public ADAG addDependency(AbstractJob parent, AbstractJob child, String label) {
        addDependency(parent.getId(), child.getId(), label);
        return this;
    }

    /**
     * Generate a DAX File out of this object in YAML format.
     *
     * @param daxfile The file to write the DAX to
     */
    public void writeToFile(String daxfile) {
        this.writeToFile(daxfile, DEFAULT_FORMAT);
    }

    /**
     * Generate a DAX File out of this object;
     *
     * @param daxfile The file to write the DAX to
     * @param format how should the file be formatted
     */
    public void writeToFile(String daxfile, FORMAT format) {
        try {
            this.writeTo(new FileWriter(daxfile), format);
        } catch (IOException ioe) {
            throw new RuntimeException(
                    "Error encountered while writting out the abstract workflow to file " + daxfile,
                    ioe);
        }
    }

    /**
     * Generate a DAX File out of this object;
     *
     * @param writer the writer to the dax file
     * @param format how should the file be formatted
     */
    public void writeTo(Writer writer, FORMAT format) {
        try {
            if (null == format) {
                throw new RuntimeException("Unsupported format " + format);
            }

            switch (format) {
                case xml:
                    mWriter = new XMLWriter(writer);
                    toXML(mWriter);
                    mWriter.close();
                    break;

                case yaml:
                    // default starting 5.0 is yaml format
                    ObjectMapper mapper =
                            new ObjectMapper(
                                    new YAMLFactory().enable(YAMLGenerator.Feature.INDENT_ARRAYS));
                    mapper.configure(MapperFeature.ALLOW_COERCION_OF_SCALARS, false);
                    mapper.writeValue(writer, this);
                    break;

                default:
                    throw new RuntimeException("Unsupported format " + format);
            }
        } catch (IOException ioe) {
            throw new RuntimeException(
                    "Error encountered while writting out the abstract workflow to writer "
                            + writer
                            + " in format "
                            + format,
                    ioe);
        }
    }

    /** Convenience function to write out the generated DAX to stdout in YAML format */
    public void writeToSTDOUT() {
        this.writeTo(new BufferedWriter(new OutputStreamWriter(System.out)), DEFAULT_FORMAT);
    }

    /**
     * Convenience function to write out the generated DAX to stdout
     *
     * @param format how should the file be formatted
     */
    public void writeToSTDOUT(FORMAT format) {
        this.writeTo(new BufferedWriter(new OutputStreamWriter(System.out)), format);
    }

    /**
     * Generate a DAX representation and pipe it into the Writer
     *
     * @param writer A Writer object
     * @param close Whether writer should be closed on return.
     * @deprecated
     */
    public void writeToWriter(Writer writer, boolean close) {
        mWriter = new XMLWriter(writer);
        toXML(mWriter);
        if (close) {
            mWriter.close();
        }
    }

    /**
     * Generates the YAML representation of a workflow.
     *
     * @return YAML representation of this ADAG as a String
     */
    public String toYAML() {
        StringWriter writer = new StringWriter();
        this.writeTo(writer, FORMAT.yaml);
        return writer.toString();
    }

    /**
     * Generates a DAX representation.
     *
     * @param writer @
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

        // add metadata attributes
        writer.writeXMLComment(
                "Section 1: Metadata attributes for the workflow (can be empty) ", true);
        for (MetaData md : this.mMetaDataAttributes) {
            md.toXML(writer, indent + 1);
        }

        // print notification invokes
        writer.writeXMLComment(
                "Section 2: Invokes - Adds notifications for a workflow (can be empty)", true);
        for (Invoke i : mInvokes) {
            i.toXML(writer, indent + 1);
        }
        // print file
        writer.writeXMLComment("Section 3: Files - Acts as a Replica Catalog (can be empty)", true);
        for (File f : mFiles) {
            f.toXML(writer, indent + 1);
        }

        // print executable
        writer.writeXMLComment(
                "Section 4: Executables - Acts as a Transformaton Catalog (can be empty)", true);
        for (Executable e : mExecutables) {
            e.toXML(writer, indent + 1);
        }

        // print transformation
        writer.writeXMLComment(
                "Section 5: Transformations - Aggregates executables and Files (can be empty)",
                true);
        for (Transformation t : mTransformations) {
            t.toXML(writer, indent + 1);
        }
        // print jobs, daxes and dags
        writer.writeXMLComment(
                "Section 6: Job's, DAX's or Dag's - Defines a JOB or DAX or DAG (Atleast 1 required)",
                true);
        for (AbstractJob j : mJobs.values()) {
            j.toXML(writer, indent + 1);
        }
        // print dependencies
        writer.writeXMLComment(
                "Section 7: Dependencies - Parent Child relationships (can be empty)", true);

        for (String child : mDependencies.keySet()) {
            writer.startElement("child", indent + 1).writeAttribute("ref", child);
            for (Edge e : mDependencies.get(child)) {
                e.toXMLParent(writer, indent + 2);
            }
            writer.endElement(indent + 1);
        }
        // end adag
        writer.endElement();
    }

    /**
     * Custom serializer for YAML representation of ADAG
     *
     * @author Ryan Tanaka
     */
    static class JsonSerializer extends PegasusJsonSerializer<ADAG> {
        public JsonSerializer() {}

        /**
         * Serializes ADAG into YAML representation
         *
         * @param adag
         * @param gen
         * @param sp
         * @throws IOException
         */
        public void serialize(ADAG adag, JsonGenerator gen, SerializerProvider sp)
                throws IOException {
            gen.writeStartObject();
            // pegasus
            gen.writeStringField("pegasus", "5.0");

            // write out pegasus vendor extensions
            gen.writeFieldName(ADAG.PEGASUS_VENDOR_EXTENSION_KEY);
            gen.writeObject(adag.getPegasusVendorExtensions());

            // name
            gen.writeStringField("name", adag.mName);

            // hooks
            if (!adag.mInvokes.isEmpty()) {
                gen.writeObjectFieldStart("hooks");
                gen.writeArrayFieldStart("shell");
                for (Invoke iv : adag.mInvokes) {
                    gen.writeObject(iv);
                }
                gen.writeEndArray();
                gen.writeEndObject();
            }

            // metadata
            if (!adag.mMetaDataAttributes.isEmpty()) {
                Map<String, String> md = new HashMap<>();
                for (MetaData m : adag.mMetaDataAttributes) {
                    md.put(m.getKey(), m.getValue());
                }
                gen.writeFieldName("metadata");
                gen.writeObject(md);
            }

            // replica catalog if specified
            if (!adag.mFiles.isEmpty()) {
                // create a ReplicaStore object that can serialize all the files
                ReplicaStore store = new ReplicaStore();
                for (File f : adag.getFiles()) {
                    store.add(f.toReplicaLocation());
                }
                // PM-1669 set the version to null so that it is not
                // serialized out . the RC in included in the workflow
                // description here
                store.setVersion(null);
                gen.writeFieldName("replicaCatalog");
                gen.writeObject(store);
            }

            // transformation catalog if specified
            if (!adag.mExecutables.isEmpty()) {
                // create a ReplicaStore object that can serialize all the files
                TransformationStore store = new TransformationStore();
                // PM-1669 set the version to null so that it is not
                // serialized out . the tc in included in the workflow
                // description here
                store.setVersion(null);
                for (Executable ex : adag.getExecutables()) {
                    for (TransformationCatalogEntry entry : ex.toTransformationCatalogEntries()) {
                        store.addEntry(entry);
                    }
                }
                gen.writeFieldName("transformationCatalog");
                gen.writeObject(store);
            }

            // jobs
            gen.writeArrayFieldStart("jobs");
            for (AbstractJob j : adag.mJobs.values()) {
                gen.writeObject(j);
            }
            gen.writeEndArray();

            // dependencies
            if (!adag.mDependencies.isEmpty()) {
                // mDependencies is given as map of child ids with
                // sets of edges. In the YAML schema, this is flipped
                // where each parent is given and a list of their
                // children. To preserve the toXML function, mDependencies
                // is being left as is and we are re-mapping things here.
                Map<String, List<String>> deps = new HashMap<>();

                for (Map.Entry<String, Set<Edge>> child : adag.mDependencies.entrySet()) {
                    for (Edge e : child.getValue()) {
                        if (deps.containsKey(e.getParent())) {
                            deps.get(e.getParent()).add(e.getChild());
                        } else {
                            List<String> children = new ArrayList<>();
                            children.add(e.getChild());
                            deps.put(e.getParent(), children);
                        }
                    }
                }

                gen.writeArrayFieldStart("jobDependencies");

                for (Map.Entry<String, List<String>> parent : deps.entrySet()) {
                    gen.writeStartObject();

                    gen.writeStringField("id", parent.getKey());

                    gen.writeArrayFieldStart("children");
                    for (String child : parent.getValue()) {
                        gen.writeString(child);
                    }
                    gen.writeEndArray();

                    gen.writeEndObject();
                }

                gen.writeEndArray();
            }

            gen.writeEndObject();
        }
    }

    /**
     * Create an example DIAMOND DAX
     *
     * @param args
     */
    public static void main(String[] args) {
        String dax = "diamond.dax";
        if (args.length > 0) {
            dax = args[0];
            Diamond().writeToFile(dax);

            Callback c = new DAX2CDAG();
            PegasusBag bag = new PegasusBag();
            bag.add(PegasusBag.PEGASUS_PROPERTIES, PegasusProperties.nonSingletonInstance());
            bag.add(PegasusBag.PEGASUS_LOGMANAGER, LogManager.getInstance("", ""));
            bag.getLogger().setLevel(Level.DEBUG);
            DAXParser5 parser = new DAXParser5(bag, "5.0");
            c.initialize(bag, dax);
            System.err.println("Validation of file " + parser.validate(dax));

        } else {
            Diamond().writeToSTDOUT(FORMAT.yaml);
        }
    }

    private static ADAG Diamond() {
        ADAG dax = new ADAG("test");

        File fa = new File("f.a");
        fa.addMetaData("foo", "bar");
        fa.addMetaData("num", "1");
        fa.addProfile("env", "FOO", "/usr/bar");
        fa.addProfile("globus", "walltime", "40");
        fa.addPhysicalFile("file:///scratch/f.a", "local");
        dax.addFile(fa);

        File fb1 = new File("f.b1");
        fb1.addMetaData("foo", "bar");
        fb1.addMetaData("num", "2");
        fb1.addProfile("env", "GOO", "/usr/foo");
        fb1.addProfile("globus", "walltime", "40");
        dax.addFile(fb1);

        File fb2 = new File("f.b2");
        fb2.addMetaData("foo", "bar");
        fb2.addMetaData("num", "3");
        fb2.addProfile("env", "BAR", "/usr/goo");
        fb2.addProfile("globus", "walltime", "40");
        dax.addFile(fb2);

        File fc1 = new File("f.c1");
        fc1.addProfile("env", "TEST", "/usr/bin/true");
        fc1.addProfile("globus", "walltime", "40");
        dax.addFile(fc1);

        File fc2 = new File("f.c2");
        fc2.addMetaData("foo", "bar");
        fc2.addMetaData("num", "5");
        dax.addFile(fc2);

        File fd = new File("f.d");
        dax.addFile(fd);

        Executable preprocess = new Executable("pegasus", "preproces", "1.0");
        preprocess.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
        preprocess.setInstalled(false);
        preprocess.addPhysicalFile(new PFN("file:///opt/pegasus/default/bin/keg", "local"));
        preprocess.addProfile(Profile.NAMESPACE.globus, "walltime", "120");
        preprocess.addMetaData("project", "pegasus");

        Executable findrange = new Executable("pegasus", "findrange", "1.0");
        findrange.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
        findrange.unsetInstalled();
        findrange.addPhysicalFile(new PFN("http://pegasus.isi.edu/code/bin/keg", "local"));
        findrange.addProfile(Profile.NAMESPACE.globus, "walltime", "120");
        findrange.addMetaData("project", "pegasus");

        Executable analyze = new Executable("pegasus", "analyze", "1.0");
        analyze.setArchitecture(Executable.ARCH.X86).setOS(Executable.OS.LINUX);
        analyze.unsetInstalled();
        analyze.addPhysicalFile(new PFN("gsiftp://localhost/opt/pegasus/default/bin/keg", "local"));
        analyze.addProfile(Profile.NAMESPACE.globus, "walltime", "120");
        analyze.addMetaData("project", "pegasus");

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
        j1.uses("f.b2", File.LINK.OUTPUT);
        j1.addProfile(Profile.NAMESPACE.dagman, "pre", "20");
        j1.addInvoke(
                WHEN.start,
                "/usr/local/pegasus/libexec/notification/email -t notify@example.com -f workflow@example.com");
        j1.addInvoke(
                WHEN.at_end,
                "/usr/local/pegasus/libexec/notification/email -t notify@example.com -f workflow@example.com");
        dax.addJob(j1);

        DAG j2 = new DAG("j2", "findrange.dag", "j2");
        j2.uses(new File("f.b1"), File.LINK.INPUT);
        j2.uses("f.c1", File.LINK.OUTPUT, File.TRANSFER.FALSE, false);
        j2.addProfile(Profile.NAMESPACE.dagman, "pre", "20");
        j2.addProfile("condor", "universe", "vanilla");
        j2.addInvoke(
                WHEN.start,
                "/usr/local/pegasus/libexec/notification/email -t notify@example.com -f workflow@example.com");
        j2.addInvoke(
                WHEN.at_end,
                "/usr/local/pegasus/libexec/notification/email -t notify@example.com -f workflow@example.com");
        dax.addDAG(j2);

        DAX j3 = new DAX("j3", "findrange.dax", "j3");
        j3.addArgument("--site ").addArgument("local");
        j3.uses(new File("f.b2"), File.LINK.INPUT, "");
        j3.uses(new File("f.c2"), File.LINK.OUTPUT, File.TRANSFER.FALSE, false, false, false, "30");
        j3.addInvoke(Invoke.WHEN.start, "/bin/notify -m START gmehta@isi.edu");
        j3.addInvoke(Invoke.WHEN.at_end, "/bin/notify -m END gmehta@isi.edu");
        j3.addInvoke(
                WHEN.start,
                "/usr/local/pegasus/libexec/notification/email -t notify@example.com -f workflow@example.com");
        j3.addInvoke(
                WHEN.at_end,
                "/usr/local/pegasus/libexec/notification/email -t notify@example.com -f workflow@example.com");
        j3.addProfile("ENV", "HAHA", "YADAYADAYADA");
        dax.addDAX(j3);

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
        j4.addInvoke(
                WHEN.start,
                "/usr/local/pegasus/libexec/notification/email -t notify@example.com -f workflow@example.com");
        j4.addInvoke(
                WHEN.at_end,
                "/usr/local/pegasus/libexec/notification/email -t notify@example.com -f workflow@example.com");
        dax.addJob(j4);

        dax.addDependency("j1", "j2", "1-2");
        dax.addDependency("j1", "j3", "1-3");
        dax.addDependency("j2", "j4");
        dax.addDependency("j3", "j4");
        return dax;
    }
}
