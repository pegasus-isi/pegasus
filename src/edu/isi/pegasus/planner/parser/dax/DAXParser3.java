/*
 *
 *   Copyright 2007-2008 University Of Southern California
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 */

package edu.isi.pegasus.planner.parser.dax;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LoggingKeys;
import edu.isi.pegasus.common.util.CondorVersion;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.impl.Abstract;
import edu.isi.pegasus.planner.classes.CompoundTransformation;
import edu.isi.pegasus.planner.classes.DAGJob;
import edu.isi.pegasus.planner.classes.DAXJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.Notifications;
import edu.isi.pegasus.planner.classes.PCRelation;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PegasusFile.LINKAGE;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.classes.ReplicaLocation;
import edu.isi.pegasus.planner.code.GridStartFactory;
import edu.isi.pegasus.planner.common.VariableExpansionReader;
import edu.isi.pegasus.planner.dax.Executable;
import edu.isi.pegasus.planner.dax.Executable.ARCH;
import edu.isi.pegasus.planner.dax.Executable.OS;
import edu.isi.pegasus.planner.dax.Invoke;
import edu.isi.pegasus.planner.dax.Invoke.WHEN;
import edu.isi.pegasus.planner.dax.MetaData;
import edu.isi.pegasus.planner.dax.PFN;
import edu.isi.pegasus.planner.namespace.Hints;
import edu.isi.pegasus.planner.namespace.Pegasus;
import edu.isi.pegasus.planner.parser.Parser;
import edu.isi.pegasus.planner.parser.StackBasedXMLParser;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * This class uses the Xerces SAX2 parser to validate and parse an XML document conforming to the
 * DAX Schema 3.2
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */
public class DAXParser3 extends StackBasedXMLParser implements DAXParser {

    /** The "not-so-official" location URL of the DAX Parser Schema. */
    public static final String DEFAULT_SCHEMA_LOCATION =
            "http://pegasus.isi.edu/schema/dax-3.6.xsd";
    /** The "not-so-official" location URL of the DAX Parser Schema. */
    public static final String SCHEMA_LOCATION_DIRECTORY = "http://pegasus.isi.edu/schema/";

    /** uri namespace */
    public static final String SCHEMA_NAMESPACE = "http://pegasus.isi.edu/schema/DAX";

    /** Constant denoting an undefined site */
    public static final String UNDEFINED_SITE = "undefined";

    /*
     * Predefined Constant for dax version 3.2.0
     */
    public static final long DAX_VERSION_3_2_0 = CondorVersion.numericValue("3.2.0");

    /*
     * Predefined Constant  for dax version 3.3.0
     */
    public static final long DAX_VERSION_3_3_0 = CondorVersion.numericValue("3.3.0");

    /*
     * Predefined Constant  for dax version 3.4.0
     */
    public static final long DAX_VERSION_3_4_0 = CondorVersion.numericValue("3.4.0");

    /*
     * Predefined Constant  for dax version 3.5.0
     */
    public static final long DAX_VERSION_3_5_0 = CondorVersion.numericValue("3.5.0");

    /*
     * Predefined Constant  for dax version 3.6.0
     */
    public static final long DAX_VERSION_3_6_0 = CondorVersion.numericValue("3.6.0");

    /** Constant denoting default metadata type */
    private String DEFAULT_METADATA_TYPE = "String";

    /** List of parents for a child node in the graph */
    protected List<PCRelation> mParents;

    /** Handle to the callback */
    protected Callback mCallback;

    /** Schema version of the DAX as detected in the factory. */
    protected String mSchemaVersion;

    /**
     * The overloaded constructor. The schema version passed is determined in the DAXFactory
     *
     * @param bag
     * @param schemaVersion the schema version specified in the DAX file.
     */
    public DAXParser3(PegasusBag bag, String schemaVersion) {
        super(bag);
        mSchemaVersion = schemaVersion;
    }

    /**
     * Set the DAXCallback for the parser to call out to.
     *
     * @param c the callback
     */
    public void setDAXCallback(Callback c) {
        this.mCallback = c;
    }

    /**
     * Returns the DAXCallback for the parser
     *
     * @return the callback
     */
    public Callback getDAXCallback() {
        return this.mCallback;
    }

    /**
     * The main method that starts the parsing.
     *
     * @param file the XML file to be parsed.
     */
    public void startParser(String file) {
        mLogger.logEventStart(LoggingKeys.EVENT_PEGASUS_PARSE_DAX, LoggingKeys.DAX_ID, file);
        try {
            // PM-938 set the schema location. we cannot set it in constructor
            this.setSchemaLocations();

            this.testForFile(file);

            // PM-831 set up the parser with our own reader
            // that allows for parameter expansion before
            // doing any XML processing
            InputSource is = new InputSource(new VariableExpansionReader(new FileReader(file)));
            mParser.parse(is);

            // sanity check
            if (mDepth != 0) {
                throw new RuntimeException("Invalid stack depth at end of parsing " + mDepth);
            }
        } catch (IOException ioe) {
            mLogger.log("IO Error :" + ioe.getMessage(), LogManager.ERROR_MESSAGE_LEVEL);
        } catch (SAXException se) {

            if (mLocator != null) {
                mLogger.log(
                        "Error in "
                                + mLocator.getSystemId()
                                + " at line "
                                + mLocator.getLineNumber()
                                + " at column "
                                + mLocator.getColumnNumber()
                                + " :"
                                + se.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            }
        }
        mLogger.logEventCompletion();
    }

    /**
     * Returns the XML schema namespace that a document being parsed conforms to.
     *
     * @return the schema namespace
     */
    public String getSchemaNamespace() {
        return DAXParser3.SCHEMA_NAMESPACE;
    }

    /**
     * Returns the local path to the XML schema against which to validate.
     *
     * @return path to the schema
     */
    public String getSchemaLocation() {
        // treat URI as File, yes, I know - I need the basename
        File uri = new File("dax-" + mSchemaVersion + ".xsd");
        // create a pointer to the default local position
        File dax = new File(this.mProps.getSchemaDir(), uri.getName());

        return this.mProps.getDAXSchemaLocation(dax.getAbsolutePath());
    }

    /**
     * Composes the <code>SiteData</code> object corresponding to the element name in the XML
     * document.
     *
     * @param element the element name encountered while parsing.
     * @param names is a list of attribute names, as strings.
     * @param values is a list of attribute values, to match the key list.
     * @return the relevant SiteData object, else null if unable to construct.
     * @exception IllegalArgumentException if the element name is too short.
     */
    public Object createObject(String element, List names, List values) {

        if (element == null || element.length() < 1) {
            throw new IllegalArgumentException("illegal element length");
        }

        switch (element.charAt(0)) {
                // a adag argument
            case 'a':
                if (element.equals("adag")) {
                    // for now the adag element is just a map of
                    // key value pair
                    Map<String, String> m = new HashMap();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);
                        if (name.equals("name")) {
                            // PM-1262 make the name dagman compliant
                            value = Parser.makeDAGManCompliant(value);
                        }
                        m.put(name, value);
                    }

                    sanityCheckOnVersion(m.get("version"));

                    // put the call to the callback
                    this.mCallback.cbDocument(m);
                    return m;
                } // end of element adag
                else if (element.equals("argument")) {
                    // arguments are constructed from character data
                    // since we don't trim it by default, reset text content
                    // buffer explicitly at start of arguments tag
                    mTextContent.setLength(0);
                    return new Arguments();
                }
                return null;

                // c child compound
            case 'c':
                if (element.equals("child")) {
                    this.mParents = new LinkedList<PCRelation>();
                    PCRelation pc = new PCRelation();
                    String child = null;
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("ref")) {
                            child = value;
                        }
                    }
                    if (child == null) {
                        this.complain(element, "child", child);
                        return null;
                    }
                    pc.setChild(child);
                    return pc;
                } else if (element.equals("compound")) {

                }
                return null;

                // d dag dax
            case 'd':
                if (element.equals("dag") || element.equals("dax")) {
                    Job j = new Job();
                    // all jobs in the DAX are of type compute
                    j.setUniverse(GridGateway.JOB_TYPE.compute.toString());
                    j.setJobType(Job.COMPUTE_JOB);

                    String file = null;
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("namespace")) {
                            j.setTXNamespace(value);
                        } else if (name.equals("name")) {
                            j.setTXName(value);
                        } else if (name.equals("version")) {
                            j.setTXVersion(value);
                        } else if (name.equals("id")) {
                            j.setLogicalID(value);
                        } else if (name.equals("file")) {
                            file = value;
                        } else if (name.equals("node-label")) {
                            j.setNodeLabel(value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }

                    if (file == null) {
                        this.complain(element, "file", file);
                        return null;
                    }
                    PegasusFile pf = new PegasusFile(file);
                    pf.setLinkage(LINKAGE.input);

                    if (element.equals("dag")) {
                        DAGJob dagJob = new DAGJob(j);

                        // we dont want notifications to be inherited
                        dagJob.resetNotifications();

                        dagJob.setDAGLFN(file);
                        dagJob.addInputFile(pf);

                        // the job should always execute on local site
                        // for time being
                        dagJob.hints.construct(Hints.EXECUTION_SITE_KEY, "local");

                        // also set the executable to be used
                        dagJob.hints.construct(Hints.PFN_HINT_KEY, "/opt/condor/bin/condor-dagman");

                        // add default name and namespace information
                        dagJob.setTransformation("condor", "dagman", null);

                        dagJob.setDerivation("condor", "dagman", null);

                        dagJob.level = -1;

                        // dagman jobs are always launched without a gridstart
                        dagJob.vdsNS.construct(
                                Pegasus.GRIDSTART_KEY,
                                GridStartFactory.GRIDSTART_SHORT_NAMES[
                                        GridStartFactory.NO_GRIDSTART_INDEX]);

                        // set the internal primary id for job
                        // dagJob.setName( constructJobID( dagJob ) );
                        return dagJob;
                    } else if (element.equals("dax")) {
                        DAXJob daxJob = new DAXJob(j);

                        // we dont want notifications to be inherited
                        daxJob.resetNotifications();

                        // the job should be tagged type pegasus
                        daxJob.setTypeRecursive();

                        // the job should always execute on local site
                        // for time being
                        daxJob.hints.construct(Hints.EXECUTION_SITE_KEY, "local");

                        // also set a fake executable to be used
                        daxJob.hints.construct(Hints.PFN_HINT_KEY, "/tmp/pegasus-plan");

                        // retrieve the extra attribute about the DAX
                        daxJob.setDAXLFN(file);
                        daxJob.addInputFile(pf);

                        // add default name and namespace information
                        daxJob.setTransformation(
                                "pegasus", "pegasus-plan", Version.instance().toString());

                        daxJob.setDerivation(
                                "pegasus", "pegasus-plan", Version.instance().toString());

                        daxJob.level = -1;
                        return daxJob;
                    }
                } // end of element job
                return null; // end of j

                // e executable
            case 'e':
                if (element.equals("executable")) {
                    String namespace = null;
                    String execName = null;
                    String version = null;
                    ARCH arch = null;
                    OS os = null;
                    String os_release = null;
                    String os_version = null;
                    String os_glibc = null;
                    Boolean os_installed = true; // Default is installed
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("namespace")) {
                            namespace = value;
                        } else if (name.equals("name")) {
                            execName = value;
                        } else if (name.equals("version")) {
                            version = value;
                        } else if (name.equals("arch")) {
                            arch = Executable.ARCH.valueOf(value.toLowerCase());
                        } else if (name.equals("os")) {
                            os = Executable.OS.valueOf(value.toLowerCase());
                        } else if (name.equals("osrelease")) {
                            os_release = value;
                        } else if (name.equals("osversion")) {
                            os_version = value;
                        } else if (name.equals("glibc")) {
                            os_glibc = value;
                        } else if (name.equals("installed")) {
                            os_installed = Boolean.parseBoolean(value);
                        }
                    }
                    Executable executable = new Executable(namespace, execName, version);
                    executable.setArchitecture(arch);
                    executable.setOS(os);
                    executable.setOSRelease(os_release);
                    executable.setOSVersion(os_version);
                    executable.setGlibc(os_glibc);
                    executable.setInstalled(os_installed);
                    return executable;
                } // end of element executable

                return null; // end of e

                // f file
            case 'f':
                if (element.equals("file")) {
                    // create a FileTransfer Object or shd it be ReplicaLocations?
                    // FileTransfer ft = new FileTransfer();
                    ReplicaLocation rl = new ReplicaLocation();

                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("name")) {
                            // ft.setLFN( value );
                            rl.setLFN(value);
                        } else if (name.equals("link")) {
                            // ignore dont need to do anything
                        } else if (name.equals("optional")) {
                            Boolean optional = Boolean.parseBoolean(value);
                            if (optional) {
                                // replica location object does not handle
                                // optional attribute right now.
                                // ft.setFileOptional();
                            }
                        } else {
                            this.complain(element, name, value);
                        }
                    }

                    return rl;
                } // end of element file

                return null; // end of f

                // i invoke
            case 'i':
                if (element.equals("invoke")) {

                    String when = null;
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("when")) {
                            when = value;
                            this.log(element, name, value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }
                    if (when == null) {
                        this.complain(element, "when", when);
                        return null;
                    }
                    return new Invoke(WHEN.valueOf(when));
                } // end of element invoke
                return null;

                // j job
            case 'j':
                if (element.equals("job")) {
                    Job j = new Job();
                    // all jobs in the DAX are of type compute
                    j.setUniverse(GridGateway.JOB_TYPE.compute.toString());
                    j.setJobType(Job.COMPUTE_JOB);

                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("namespace")) {
                            j.setTXNamespace(value);
                        } else if (name.equals("name")) {
                            j.setTXName(value);
                        } else if (name.equals("version")) {
                            j.setTXVersion(value);
                        } else if (name.equals("id")) {
                            j.setLogicalID(value);
                        } else if (name.equals("node-label")) {
                            j.setNodeLabel(value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }

                    return j;
                } // end of element job
                return null; // end of j

                // m metadata
            case 'm':
                if (element.equals("metadata")) {
                    Profile p = new Profile();
                    p.setProfileNamespace("metadata");
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);
                        if (name.equals("namespace")) {
                            p.setProfileNamespace(value);
                            this.log(element, name, value);
                        } else if (name.equals("key")) {
                            p.setProfileKey(value);
                            this.log(element, name, value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }
                    return p;
                } // end of element metadata

                return null; // end of case m

                // p parent profile pfn
            case 'p':
                if (element.equals("parent")) {
                    String parent = null;

                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("ref")) {
                            parent = value;
                        } else if (name.equals("edge-label")) {
                            this.attributeNotSupported("parent", "edge-label", value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }
                    if (parent == null) {
                        this.complain(element, "parent", parent);
                        return null;
                    }
                    return parent;

                } else if (element.equals("profile")) {
                    Profile p = new Profile();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("namespace")) {
                            p.setProfileNamespace(value.toLowerCase());
                            this.log(element, name, value);
                        } else if (name.equals("key")) {
                            p.setProfileKey(value);
                            this.log(element, name, value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }
                    return p;
                } // end of element profile
                else if (element.equals("pfn")) {

                    String url = null;
                    String site = UNDEFINED_SITE;
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("url")) {
                            url = value;
                            this.log(element, name, value);
                        } else if (name.equals("site")) {
                            site = value;
                            this.log(element, name, value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }
                    if (url == null) {
                        this.complain(element, "url", url);
                        return null;
                    }
                    PFN pfn = new PFN(url, site);
                    return pfn;
                } // end of element pfn
                return null; // end of case p

                // s stdin stdout stderr
            case 's':
                if (element.equals("stdin")
                        || element.equals("stdout")
                        || element.equals("stderr")) {
                    // we use DAX API File object for this
                    String fileName = null;
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("name")) {
                            fileName = value;
                            this.log(element, name, value);
                        } else if (name.equals("link")) {
                            // we ignore as linkage is fixed for stdout|stderr|stdin
                            this.log(element, name, value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }
                    if (fileName == null) {
                        this.complain(element, "name", fileName);
                        return null;
                    }
                    return new edu.isi.pegasus.planner.dax.File(fileName);
                } // end of stdin|stdout|stderr
                return null; // end of case s

                // t transformation
            case 't':
                if (element.equals("transformation")) {
                    String namespace = null, lname = null, version = null;
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("namespace")) {
                            namespace = value;
                        } else if (name.equals("name")) {
                            lname = value;
                        } else if (name.equals("version")) {
                            version = value;
                        }
                    }
                    return new CompoundTransformation(namespace, lname, version);
                }
                return null;

                // u uses
            case 'u':
                if (element.equals("uses")) {
                    PegasusFile pf = new PegasusFile();
                    String fName = null;
                    String fNamespace = null;
                    String fVersion = null;
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        /*
                        * Name  	Type  	Use  	Default  	Fixed
                           name  	xs:string  	required
                           link  	LinkageType  	optional
                           optional  	xs:boolean  	optional  	false
                           register  	xs:boolean  	optional  	true
                           transfer  	TransferType  	optional  	true
                           namespace  	xs:string  	optional
                           version  	VersionPattern  	optional
                           exectuable  	xs:boolean  	optional  	false
                        */
                        if (name.equals("name")) {
                            pf.setLFN(value);
                            fName = value;
                            this.log(element, name, value);
                        } else if (name.equals("link")) {
                            if (value != null && value.equals(PegasusFile.CHECKPOINT_TYPE)) {
                                // introduced in dax 3.5
                                // cleaner for DAX API to have checkpoint files marked
                                // via linkage. Planner still treats it as a type
                                pf.setType(value);
                                pf.setLinkage(LINKAGE.inout);
                                this.log(element, name, value);
                            } else {
                                pf.setLinkage(PegasusFile.LINKAGE.valueOf(value.toLowerCase()));
                                this.log(element, name, value);
                            }

                        } else if (name.equals("optional")) {
                            Boolean bValue = Boolean.parseBoolean(value);
                            if (bValue) {
                                pf.setFileOptional();
                            }
                            this.log(element, name, value);
                        } else if (name.equals("register")) {
                            Boolean bValue = Boolean.parseBoolean(value);
                            pf.setRegisterFlag(bValue);
                        } else if (name.equals("transfer")) {
                            pf.setTransferFlag(value);
                            this.log(element, name, value);
                        } else if (name.equals("namespace")) {
                            fNamespace = value;
                            this.log(element, name, value);
                        } else if (name.equals("version")) {
                            fVersion = value;
                            this.log(element, name, value);
                        } else if (name.equals("executable")) {
                            Boolean bValue = Boolean.parseBoolean(value);
                            if (bValue) {
                                pf.setType(PegasusFile.EXECUTABLE_FILE);
                            }
                            this.log(element, name, value);
                        } else if (name.equals("size")) {
                            pf.setSize(value);
                            this.log(element, name, value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }

                    // if executable then update lfn to combo of namespace,name,version
                    if (pf.getType() == PegasusFile.EXECUTABLE_FILE) {
                        pf.setLFN(Separator.combine(fNamespace, fName, fVersion));
                    }
                    return pf;
                } // end of uses
                return null; // end of case u

            default:
                return null;
        } // end of switch statement
    }

    /**
     * This method sets the relations between the currently finished XML element(child) and its
     * containing element in terms of Java objects. Usually it involves adding the object to the
     * parent's child object list.
     *
     * @param childElement name is the the child element name
     * @param parent is a reference to the parent's Java object
     * @param child is the completed child object to connect to the parent
     * @return true if the element was added successfully, false, if the child does not match into
     *     the parent.
     */
    public boolean setElementRelation(String childElement, Object parent, Object child) {

        switch (childElement.charAt(0)) {
                // a argument adag
            case 'a':
                if (child instanceof Arguments) {
                    Arguments a = (Arguments) child;
                    a.addArgument(mTextContent.toString());

                    if (parent instanceof Job) {
                        // argument appears in job element
                        Job j = (Job) parent;
                        j.setArguments(a.toString());
                        return true;
                    }
                } else if (child instanceof Map && parent == null) {
                    // end of parsing reached
                    mLogger.log(
                            "End of last element </adag> reached ", LogManager.DEBUG_MESSAGE_LEVEL);

                    this.mCallback.cbDone();
                    return true;
                }
                return false;

                // c child
            case 'c':
                if (parent instanceof Map) {
                    if (child instanceof PCRelation) {
                        PCRelation pc = (PCRelation) child;
                        // call the callback
                        this.mCallback.cbParents(pc.getChild(), mParents);
                        return true;
                    }
                }
                return false;

                // d dax dag
            case 'd':
                if (parent instanceof Map) {

                    if (child instanceof DAGJob) {
                        // dag appears in adag element
                        DAGJob dagJob = (DAGJob) child;

                        // call the callback function
                        this.mCallback.cbJob(dagJob);
                        return true;
                    } else if (child instanceof DAXJob) {
                        // dag appears in adag element
                        DAXJob daxJob = (DAXJob) child;

                        // call the callback function
                        this.mCallback.cbJob(daxJob);
                        return true;
                    }
                }
                return false;

                // f file
            case 'f':
                if (child instanceof ReplicaLocation) {
                    ReplicaLocation rl = (ReplicaLocation) child;
                    if (parent instanceof Map) {
                        // file appears in adag element
                        //                        this.mReplicaStore.add( rl );
                        this.mCallback.cbFile(rl);
                        return true;
                    } else if (parent instanceof Arguments) {
                        // file appears in the argument element
                        Arguments a = (Arguments) parent;
                        a.addArgument(mTextContent.toString());
                        a.addArgument(rl);
                        return true;
                    }
                }
                return false;

                // e executable
            case 'e':
                if (child instanceof Executable) {
                    if (parent instanceof Map) {
                        // executable appears in adag element
                        Executable exec = (Executable) child;
                        List<TransformationCatalogEntry> tceList = convertExecutableToTCE(exec);
                        for (TransformationCatalogEntry tce : tceList) {
                            this.mCallback.cbExecutable(Abstract.modifyForFileURLS(tce));
                        }
                        // moved the callback call to end of pfn
                        // each new pfn is a new transformation
                        // catalog entry
                        // this.mCallback.cbExecutable( tce );
                        return true;
                    }
                }
                return false;

                // i invoke
            case 'i':
                if (child instanceof Invoke) {
                    Invoke i = (Invoke) child;
                    i.setWhat(mTextContent.toString().trim());
                    if (parent instanceof Map) {
                        this.mCallback.cbWfInvoke(i);
                        return true;
                    } else if (parent instanceof DAXJob) {
                        // invoke appears in dax element
                        DAXJob daxJob = (DAXJob) parent;
                        daxJob.addNotification(i);
                        return true;
                    } else if (parent instanceof DAGJob) {
                        // invoke appears in dag element
                        DAGJob dagJob = (DAGJob) parent;
                        dagJob.addNotification(i);
                        return true;
                    } else if (parent instanceof Job) {
                        // invoke appears in job element
                        Job job = (Job) parent;
                        job.addNotification(i);
                        return true;
                    } else if (parent instanceof Executable) {
                        // invoke appears in executable element
                        Executable exec = (Executable) parent;
                        exec.addInvoke(i);
                        return true;
                    } else if (parent instanceof CompoundTransformation) {
                        // invoke appears in transformation element
                        CompoundTransformation ct = (CompoundTransformation) parent;
                        ct.addNotification(i);
                        return true;
                    }
                }
                return false;

                // j job
            case 'j':
                if (child instanceof Job && parent instanceof Map) {
                    // callback for Job
                    this.mCallback.cbJob((Job) child);
                    return true;
                }
                return false;

                // m metadata
            case 'm':
                if (child instanceof Profile) {
                    Profile md = (Profile) child;
                    md.setProfileValue(mTextContent.toString().trim());

                    if (parent instanceof Map) {
                        // metadata appears in adag element
                        this.mCallback.cbMetadata(md);
                        return true;
                    } else if (parent instanceof Job) {
                        // metadata appears in the job element
                        Job j = (Job) parent;
                        j.addProfile(md);
                        return true;
                    } else if (parent instanceof ReplicaLocation) {
                        // metadata appears in file element
                        ReplicaLocation rl = (ReplicaLocation) parent;
                        rl.addMetadata(md.getProfileKey(), md.getProfileValue());
                        return true;
                    } else if (parent instanceof Executable) {
                        // metadata appears in executable element
                        Executable e = (Executable) parent;
                        e.addMetaData(new MetaData(md.getProfileKey(), md.getProfileValue()));
                        return true;
                    } else if (parent instanceof PegasusFile) {
                        // metadata appears in uses element
                        PegasusFile pf = (PegasusFile) parent;
                        pf.addMetadata(md.getProfileKey(), md.getProfileValue());
                        return true;
                    }
                }
                return false;

                // p parent profile pfn
            case 'p':
                if (parent instanceof PCRelation) {
                    if (child instanceof String) {
                        // parent appears in child element
                        String parentNode = (String) child;
                        PCRelation pc = (PCRelation) ((PCRelation) parent).clone();
                        pc.setParent(parentNode);
                        mParents.add(pc);
                        return true;
                    }
                } else if (child instanceof Profile) {
                    Profile p = (Profile) child;
                    p.setProfileValue(mTextContent.toString().trim());
                    mLogger.log(
                            "Set Profile Value to " + p.getProfileValue(),
                            LogManager.TRACE_MESSAGE_LEVEL);
                    if (parent instanceof ReplicaLocation) {
                        // profile appears in file element
                        unSupportedNestingOfElements("file", "profile");
                        return true;
                    } else if (parent instanceof Executable) {
                        // profile appears in executable element
                        Executable exec = (Executable) parent;
                        exec.addProfiles(
                                new edu.isi.pegasus.planner.dax.Profile(
                                        p.getProfileNamespace(),
                                        p.getProfileKey(),
                                        p.getProfileValue()));
                        return true;
                    } else if (parent instanceof Job) {
                        // profile appears in the job element
                        Job j = (Job) parent;
                        j.addProfile(p);
                        return true;
                    }
                } else if (child instanceof PFN) {
                    if (parent instanceof ReplicaLocation) {
                        // pfn appears in file element
                        ReplicaLocation rl = (ReplicaLocation) parent;
                        PFN pfn = (PFN) child;
                        rl.addPFN(pfn);
                        return true;
                    } else if (parent instanceof Executable) {
                        // pfn appears in executable element
                        Executable executable = (Executable) parent;
                        PFN pfn = (PFN) child;
                        // tce.setResourceId( pfn.getSite() );
                        // tce.setPhysicalTransformation( pfn.getURL() );
                        executable.addPhysicalFile(pfn);

                        // convert file url appropriately for installed executables
                        // before returning
                        // this.mCallback.cbExecutable( Abstract.modifyForFileURLS(tce) );

                        return true;
                    }
                }
                return false;

                // s stdin stdout stderr
            case 's':
                if (parent instanceof Job) {
                    Job j = (Job) parent;

                    if (child instanceof edu.isi.pegasus.planner.dax.File) {
                        // stdin stdout stderr appear in job element
                        edu.isi.pegasus.planner.dax.File f =
                                (edu.isi.pegasus.planner.dax.File) child;

                        if (childElement.equals("stdin")) {
                            j.setStdIn(f.getName());
                            return true;
                        } else if (childElement.equals("stdout")) {
                            j.setStdOut(f.getName());
                            return true;
                        }

                        if (childElement.equals("stderr")) {
                            j.setStdErr(f.getName());
                            return true;
                        }
                    }
                }
                return false;

                // t transformation
            case 't':
                if (parent instanceof Map) {
                    if (child instanceof CompoundTransformation) {
                        this.mCallback.cbCompoundTransformation((CompoundTransformation) child);
                        return true;
                    }
                    return true;
                }
                return false;

                // u uses
            case 'u':
                if (child instanceof PegasusFile) {
                    PegasusFile pf = (PegasusFile) child;
                    if (parent instanceof Job) {
                        // uses appears in job
                        Job j = (Job) parent;

                        if (pf.getLinkage().equals(LINKAGE.input)) {
                            j.addInputFile(pf);
                        } else if (pf.getLinkage().equals(LINKAGE.output)) {
                            j.addOutputFile(pf);
                        } else if (pf.getLinkage().equals(LINKAGE.inout)) {
                            j.addInputFile(pf);
                            j.addOutputFile(pf);
                        }
                        return true;
                    } else if (parent instanceof CompoundTransformation) {
                        CompoundTransformation compound = (CompoundTransformation) parent;
                        compound.addDependantFile(pf);
                        return true;
                    }
                }
                return false;

                // default case
            default:
                return false;
        }
    }

    /**
     * Converts the executable into transformation catalog entries
     *
     * @param executable executable object
     * @return transformation catalog entries
     */
    public List<TransformationCatalogEntry> convertExecutableToTCE(Executable executable) {
        List<TransformationCatalogEntry> tceList = new ArrayList<TransformationCatalogEntry>();
        TransformationCatalogEntry tce = null;
        for (PFN pfn : executable.getPhysicalFiles()) {
            tce =
                    new TransformationCatalogEntry(
                            executable.getNamespace(),
                            executable.getName(),
                            executable.getVersion());
            SysInfo sysinfo = new SysInfo();
            sysinfo.setArchitecture(
                    SysInfo.Architecture.valueOf(
                            executable.getArchitecture().toString().toLowerCase()));
            sysinfo.setOS(SysInfo.OS.valueOf(executable.getOS().toString().toLowerCase()));
            sysinfo.setOSRelease(executable.getOsRelease());
            sysinfo.setOSVersion(executable.getOsVersion());
            sysinfo.setGlibc(executable.getGlibc());
            tce.setSysInfo(sysinfo);
            tce.setType(executable.getInstalled() ? TCType.INSTALLED : TCType.STAGEABLE);
            tce.setResourceId(pfn.getSite());
            tce.setPhysicalTransformation(pfn.getURL());
            Notifications notifications = new Notifications();
            for (Invoke invoke : executable.getInvoke()) {
                notifications.add(new Invoke(invoke));
            }
            tce.addNotifications(notifications);
            for (edu.isi.pegasus.planner.dax.Profile profile : executable.getProfiles()) {
                tce.addProfile(
                        new edu.isi.pegasus.planner.classes.Profile(
                                profile.getNameSpace(), profile.getKey(), profile.getValue()));
            }
            for (MetaData md : executable.getMetaData()) {
                // convert to metadata profile object for planner to use
                tce.addProfile(new Profile(Profile.METADATA, md.getKey(), md.getValue()));
            }
            tceList.add(tce);
        }

        return tceList;
    }

    /**
     * Sanity check on the version that this parser works on.
     *
     * @param version the version as specified in the DAX
     */
    protected void sanityCheckOnVersion(String version) {
        if (version == null) {
            mLogger.log(
                    "Version not specified in the adag element ", LogManager.WARNING_MESSAGE_LEVEL);
            return;
        }

        // add a 0 suffix
        String nversion = version + ".0";
        long value = CondorVersion.numericValue(nversion);
        if (value < DAXParser3.DAX_VERSION_3_2_0) {
            StringBuffer sb = new StringBuffer();
            sb.append("DAXParser3 Unsupported DAX Version ")
                    .append(version)
                    .append(". Set pegasus.schema.dax property to load the old DAXParser");
            throw new RuntimeException(sb.toString());
        }
        // also complain for parsing documents that have version higher
        if (value > DAXParser3.DAX_VERSION_3_6_0) {
            throw new RuntimeException(
                    "DAXParser3 cannot parse documents conforming to DAX version " + version);
        }
        return;
    }

    /** Private class to handle mix data content for arguments tags. */
    private class Arguments {

        /** Handle to a job arguments to handle mixed content. */
        protected StringBuffer mBuffer;

        /** The default constructor */
        public Arguments() {
            reset();
        }

        /** Resets the internal buffer */
        public void reset() {
            mBuffer = new StringBuffer();
        }

        /**
         * Adds text to the arguments string
         *
         * @param text the text to be added.
         */
        public void addArgument(String text) {
            mBuffer.append(text);
        }

        /**
         * Adds filename to the arguments
         *
         * @param rl the ReplicaLocation object
         */
        public void addArgument(ReplicaLocation rl) {
            mBuffer.append(rl.getLFN());
        }

        /**
         * Our own implementation for ignorable whitespace. A String that holds the contents of data
         * passed as text by the underlying parser. The whitespaces at the end are replaced by one
         * whitespace.
         *
         * @param str The string that contains whitespaces.
         * @return String corresponding to the trimmed version.
         */
        public String ignoreWhitespace(String str) {
            return ignoreWhitespace(str, mProps.preserveParserLineBreaks());
        }

        /**
         * Our own implementation for ignorable whitespace. A String that holds the contents of data
         * passed as text by the underlying parser. The whitespaces at the end are replaced by one
         * whitespace.
         *
         * @param str The string that contains whitespaces.
         * @return String corresponding to the trimmed version.
         */
        public String ignoreWhitespace(String str, boolean preserveLineBreak) {
            boolean st = false;
            boolean end = false;
            int length = str.length();
            boolean sN = false; // start with \n ;
            boolean eN = false; // end with \n

            if (length > 0) {
                sN = str.charAt(0) == '\n';
                eN = str.charAt(length - 1) == '\n';
                // check for whitespace in the
                // starting
                if (str.charAt(0) == ' ' || str.charAt(0) == '\t' || str.charAt(0) == '\n') {
                    st = true;
                }
                // check for whitespace in the end
                if (str.length() > 1
                        && (str.charAt(length - 1) == ' '
                                || str.charAt(length - 1) == '\t'
                                || str.charAt(length - 1) == '\n')) {

                    end = true;
                }
                // trim the string and add a single whitespace accordingly
                str = str.trim();
                str = st == true ? ' ' + str : str;
                str = end == true ? str + ' ' : str;

                if (preserveLineBreak) {
                    str = sN ? '\n' + str : str;
                    str = eN ? str + '\n' : str;
                }
            }

            return str;
        }

        /**
         * Returns the arguments as string
         *
         * @return the arguments
         */
        public String toString() {
            return this.ignoreWhitespace(mBuffer.toString());
        }
    }

    /** @param args */
    public static void main(String[] args) {
        LogManagerFactory.loadSingletonInstance().setLevel(5);
        /*DAXParser3 parser = new DAXParser3(  );
        if (args.length == 1) {
            parser.startParser( args[0] );

        } else {
            System.out.println("Usage: SiteCatalogParser <input site catalog xml file>");
        }*/

    }
}
