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

package edu.isi.pegasus.planner.parser;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.site.classes.Connection;
import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType.OPERATION;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.HeadNodeFS;
import edu.isi.pegasus.planner.catalog.site.classes.HeadNodeScratch;
import edu.isi.pegasus.planner.catalog.site.classes.HeadNodeStorage;
import edu.isi.pegasus.planner.catalog.site.classes.InternalMountPoint;
import edu.isi.pegasus.planner.catalog.site.classes.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.site.classes.SharedDirectory;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteData;
import edu.isi.pegasus.planner.catalog.site.classes.SiteDataVisitor;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.site.classes.StorageType;
import edu.isi.pegasus.planner.catalog.site.classes.WorkerNodeFS;
import edu.isi.pegasus.planner.catalog.site.classes.WorkerNodeScratch;
import edu.isi.pegasus.planner.catalog.site.classes.WorkerNodeStorage;
import edu.isi.pegasus.planner.catalog.site.classes.XML4PrintVisitor;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.common.VariableExpansionReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

/**
 * This class uses the Xerces SAX2 parser to validate and parse an XML document conforming to the
 * Site Catalog schema v4.0
 *
 * <p>http://pegasus.isi.edu/schema/sc-3.0.xsd
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */
public class SiteCatalogXMLParser4 extends StackBasedXMLParser implements SiteCatalogXMLParser {

    /** The "not-so-official" location URL of the Site Catalog Schema. */
    public static final String SCHEMA_LOCATION = "http://pegasus.isi.edu/schema/sc-4.2.xsd";

    /** uri namespace */
    public static final String SCHEMA_NAMESPACE = "http://pegasus.isi.edu/schema/sitecatalog";

    /** The final result constructed. */
    private SiteStore mResult;

    /** The set of sites that need to be parsed. */
    private Set<String> mSites;

    /** A boolean indicating whether to load all sites. */
    private boolean mLoadAll;

    /**
     * The default Constructor.
     *
     * @param sites the list of sites to be parsed. * means all.
     */
    /* public SiteCatalogParser( List<String> sites ) {
        this( PegasusProperties.nonSingletonInstance(), sites );
    }*/

    /**
     * The overloaded constructor.
     *
     * @param bag the bag of initialization objects.
     * @param sites the list of sites that need to be parsed. * means all.
     */
    public SiteCatalogXMLParser4(PegasusBag bag, List<String> sites) {
        super(bag);
        mStack = new Stack();
        mDepth = 0;

        mSites = new HashSet<String>();
        for (Iterator<String> it = sites.iterator(); it.hasNext(); ) {
            mSites.add(it.next());
        }
        mLoadAll = mSites.contains("*");
    }

    /**
     * Returns the constructed site store object
     *
     * @return <code>SiteStore<code> if parsing completed
     */
    public SiteStore getSiteStore() {
        if (mParsingDone) {
            return mResult;
        } else {
            throw new RuntimeException(
                    "Parsing of file needs to complete before function can be called");
        }
    }

    /**
     * The main method that starts the parsing.
     *
     * @param file the XML file to be parsed.
     */
    public void startParser(String file) {
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
            mLogger.log("Object constructed is " + mResult.toXML(), LogManager.DEBUG_MESSAGE_LEVEL);
        } catch (IOException ioe) {
            mLogger.log("IO Error :" + ioe.getMessage(), LogManager.ERROR_MESSAGE_LEVEL);
        } catch (SAXException se) {

            if (mLocator != null) {
                mLogger.log(
                        "Error in "
                                + mLocator.getSystemId()
                                + " at line "
                                + mLocator.getLineNumber()
                                + "at column "
                                + mLocator.getColumnNumber()
                                + " :"
                                + se.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
            }
        }
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

        SiteData object = null;

        switch (element.charAt(0)) {
                // a alias
            case 'a':
                if (element.equals("alias")) {
                    String alias = null;
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("name")) {
                            alias = value;
                            this.log(element, name, value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }
                    return alias;
                } else {
                    return null;
                }

                // c connection
            case 'c':
                if (element.equals("connection")) {
                    Connection c = new Connection();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("key")) {
                            c.setKey(value);
                            this.log(element, name, value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }
                    return c;
                } else {
                    return null;
                }

                // d directory
            case 'd':
                if (element.equals("directory")) {
                    Map<String, String> attributes = new HashMap();
                    Directory d = new Directory();
                    InternalMountPoint mountPoint = new InternalMountPoint();
                    d.setInternalMountPoint(mountPoint);
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("type")) {
                            d.setType(value);
                            this.log(element, name, value);
                        } else if (name.equals("path")) {
                            mountPoint.setMountPoint(value);
                        } else if (name.equals("free-size")) {
                            mountPoint.setFreeSize(value);
                        } else if (name.equals("total-size")) {
                            mountPoint.setTotalSize(value);
                        } else {
                            this.complain(element, name, value);
                        }
                        attributes.put(name, value);
                    }

                    return d;
                } else {
                    return null;
                }

                // f
            case 'f':
                if (element.equals("file-server")) {
                    FileServer fs = new FileServer();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("protocol")) {
                            fs.setProtocol(value);
                            this.log(element, name, value);
                        } else if (name.equals("url")) {
                            PegasusURL url = new PegasusURL(value);
                            fs.setURLPrefix(url.getURLPrefix());
                            fs.setProtocol(url.getProtocol());
                            fs.setMountPoint(url.getPath());

                            this.log(element, name, value);
                        } else if (name.equals("mount-point")) {
                            fs.setMountPoint(value);
                            this.log(element, name, value);
                        } else if (name.equals("operation")) {
                            fs.setSupportedOperation(OPERATION.valueOf(value));
                            this.log(element, name, value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }
                    return fs;
                } else {
                    return null;
                }

                // g  grid
            case 'g':
                if (element.equals("grid")) {
                    GridGateway gw = new GridGateway();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("arch")) {
                            gw.setArchitecture(SysInfo.Architecture.valueOf(value));
                            this.log(element, name, value);
                        } else if (name.equals("type")) {
                            gw.setType(GridGateway.TYPE.valueOf(value));
                            this.log(element, name, value);
                        } else if (name.equals("contact")) {
                            gw.setContact(value);
                            this.log(element, name, value);
                        } else if (name.equals("scheduler")) {
                            gw.setScheduler(value);
                            this.log(element, name, value);
                        } else if (name.equals("jobtype")) {
                            gw.setJobType(GridGateway.JOB_TYPE.valueOf(value));
                            this.log(element, name, value);
                        } else if (name.equals("os")) {
                            gw.setOS(SysInfo.OS.valueOf(value.toLowerCase()));
                            this.log(element, name, value);
                        } else if (name.equals("osrelease")) {
                            gw.setOSRelease(value);
                            this.log(element, name, value);
                        } else if (name.equals("osversion")) {
                            gw.setOSVersion(value);
                            this.log(element, name, value);
                        } else if (name.equals("glibc")) {
                            gw.setGlibc(value);
                            this.log(element, name, value);
                        } else if (name.equals("idle-nodes")) {
                            gw.setIdleNodes(name);
                            this.log(element, name, value);
                        } else if (name.equals("total-nodes")) {
                            gw.setTotalNodes(name);
                            this.log(element, name, value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }
                    return gw;
                } else {
                    return null;
                }

                // m metadata
            case 'm':
                if (element.equals("metadata")) {
                    Profile p = new Profile();
                    p.setProfileNamespace("metadata");
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);
                        if (name.equals("key")) {
                            p.setProfileKey(value);
                            this.log(element, name, value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }
                    return p;

                } // end of element metadata
                else {
                    return null; // end of case m
                }

                // p profile
            case 'p':
                if (element.equals("profile")) {
                    Profile p = new Profile();
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
                } else {
                    return null;
                }

                // r replica-catalog
            case 'r':
                if (element.equals("replica-catalog")) {
                    ReplicaCatalog rc = new ReplicaCatalog();
                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("type")) {
                            rc.setType(value);
                            this.log(element, name, value);
                        } else if (name.equals("url")) {
                            rc.setURL(value);
                            this.log(element, name, value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }
                    return rc;
                } else {
                    return null;
                }

                // s shared scratch storage site
            case 's':
                if (element.equals("site")) {
                    SiteCatalogEntry site = new SiteCatalogEntry();

                    for (int i = 0; i < names.size(); ++i) {
                        String name = (String) names.get(i);
                        String value = (String) values.get(i);

                        if (name.equals("arch")) {
                            site.setArchitecture(SysInfo.Architecture.valueOf(value));
                            this.log(element, name, value);
                        } else if (name.equals("os")) {
                            site.setOS(SysInfo.OS.valueOf(value.toLowerCase()));
                            this.log(element, name, value);
                        } else if (name.equals("handle")) {
                            site.setSiteHandle(value);
                            this.log(element, name, value);
                        } else if (name.equals("osrelease")) {
                            site.setOSRelease(value);
                            this.log(element, name, value);
                        } else if (name.equals("osversion")) {
                            site.setOSVersion(value);
                            this.log(element, name, value);
                        } else if (name.equals("glibc")) {
                            site.setGlibc(value);
                            this.log(element, name, value);
                        } else {
                            this.complain(element, name, value);
                        }
                    }
                    return site;
                } else if (element.equals("sitecatalog")) {
                    SiteStore catalog = new SiteStore();
                    mResult = catalog;
                    return catalog;
                } else {
                    return null;
                }
        }

        return object;
    }

    /**
     * Whether to laod a site or not in the <code>SiteStore</code>
     *
     * @param site the <code>SiteCatalogEntry</code> object.
     * @return boolean
     */
    private boolean loadSite(SiteCatalogEntry site) {
        return (mLoadAll || mSites.contains(site.getSiteHandle()));
    }

    /**
     * This method sets the relations between the currently finished XML element and its containing
     * element in terms of Java objects. Usually it involves adding the object to the parent's child
     * object list.
     *
     * @param childElement name is the the child element name
     * @param parent is a reference to the parent's Java object
     * @param child is the completed child object to connect to the parent
     * @return true if the element was added successfully, false, if the child does not match into
     *     the parent.
     */
    public boolean setElementRelation(String childElement, Object parent, Object child) {

        switch (childElement.charAt(0)) {
                // a alias
            case 'a':
                // alias only appears in replica-catalog
                if (child instanceof String && parent instanceof ReplicaCatalog) {
                    ReplicaCatalog replica = (ReplicaCatalog) parent;
                    replica.addAlias((String) child);
                    return true;
                } else {
                    return false;
                }

                // c connection
            case 'c':
                // connection only appears in replica-catalog
                if (child instanceof Connection && parent instanceof ReplicaCatalog) {
                    ReplicaCatalog replica = (ReplicaCatalog) parent;
                    Connection c = (Connection) child;
                    c.setValue(mTextContent.toString().trim());
                    replica.addConnection(c);
                    return true;
                } else {
                    return false;
                }

                // d directory
            case 'd':
                // directory appears in the site element.
                // can be multiple directorys
                if (child instanceof Directory && parent instanceof SiteCatalogEntry) {
                    SiteCatalogEntry site = (SiteCatalogEntry) parent;
                    Directory d = (Directory) child;

                    // figure out the directory type
                    site.addDirectory(d);
                    return true;
                } else {
                    return false;
                }

                // f
            case 'f':
                // file-server appears in directory now
                if (child instanceof FileServer && parent instanceof Directory) {
                    Directory directory = (Directory) parent;
                    directory.addFileServer((FileServer) child);
                    return true;
                } else {
                    return false;
                }

                // g  grid
            case 'g':
                // grid only appears in the site element
                if (child instanceof GridGateway && parent instanceof SiteCatalogEntry) {
                    SiteCatalogEntry site = (SiteCatalogEntry) parent;
                    site.addGridGateway((GridGateway) child);
                    return true;
                } else {
                    return false;
                }

                // p profile
            case 'p':
                // profile appear in file-server site head-fs worker-fs
                if (child instanceof Profile) {
                    Profile p = (Profile) child;
                    p.setProfileValue(mTextContent.toString().trim());
                    mLogger.log(
                            "Set Profile Value to " + p.getProfileValue(),
                            LogManager.TRACE_MESSAGE_LEVEL);
                    if (parent instanceof FileServer) {
                        FileServer server = (FileServer) parent;
                        server.addProfile(p);
                        return true;
                    } else if (parent instanceof HeadNodeFS) {
                        HeadNodeFS fs = (HeadNodeFS) parent;
                        fs.addProfile(p);
                        return true;
                    } else if (parent instanceof WorkerNodeFS) {
                        WorkerNodeFS fs = (WorkerNodeFS) parent;
                        fs.addProfile(p);
                        return true;
                    } else if (parent instanceof SiteCatalogEntry) {
                        SiteCatalogEntry s = (SiteCatalogEntry) parent;
                        s.addProfile(p);
                        return true;
                    }
                } else {
                    return false;
                }

                // m metadata
            case 'm':
                if (child instanceof Profile) {
                    Profile md = (Profile) child;
                    md.setProfileValue(mTextContent.toString().trim());
                    if (parent instanceof SiteCatalogEntry) {
                        // profile appears in the job element
                        SiteCatalogEntry site = (SiteCatalogEntry) parent;
                        site.addProfile(md);
                        return true;
                    }
                }
                return false;

                // r replica-catalog
            case 'r':
                // replica-catalog appear in site
                if (child instanceof ReplicaCatalog && parent instanceof SiteCatalogEntry) {
                    SiteCatalogEntry s = (SiteCatalogEntry) parent;
                    s.addReplicaCatalog((ReplicaCatalog) child);
                    return true;

                } else {
                    return false;
                }

                // s shared scratch storage site site-catalog
            case 's':
                if (child instanceof SharedDirectory) {
                    // shared appears in scratch and storage
                    if (parent instanceof StorageType) {
                        StorageType st = (StorageType) parent;
                        st.setSharedDirectory((SharedDirectory) child);
                        return true;
                    }
                } else if (child instanceof StorageType && childElement.equals("scratch")) {
                    // scratch appears in HeadNodeFS and WorkerNodeFS
                    StorageType scratch = (StorageType) child;

                    if (parent instanceof HeadNodeFS) {
                        HeadNodeFS fs = (HeadNodeFS) parent;
                        fs.setScratch(new HeadNodeScratch(scratch));
                        return true;
                    } else if (parent instanceof WorkerNodeFS) {
                        WorkerNodeFS fs = (WorkerNodeFS) parent;
                        fs.setScratch(new WorkerNodeScratch(scratch));
                        return true;
                    }
                } else if (child instanceof StorageType && childElement.equals("storage")) {
                    // storage appears in HeadNodeFS and WorkerNodeFS
                    StorageType storage = (StorageType) child;

                    if (parent instanceof HeadNodeFS) {
                        HeadNodeFS fs = (HeadNodeFS) parent;
                        fs.setStorage(new HeadNodeStorage(storage));
                        return true;
                    } else if (parent instanceof WorkerNodeFS) {
                        WorkerNodeFS fs = (WorkerNodeFS) parent;
                        fs.setStorage(new WorkerNodeStorage(storage));
                        return true;
                    }
                } else if (child instanceof SiteCatalogEntry && parent instanceof SiteStore) {
                    SiteStore c = (SiteStore) parent;

                    // add only to store if required.
                    SiteCatalogEntry site = (SiteCatalogEntry) child;
                    if (loadSite(site)) {
                        mLogger.log(
                                "Loading site in SiteStore " + site.getSiteHandle(),
                                LogManager.DEBUG_MESSAGE_LEVEL);
                        c.addEntry(site);
                    }
                    return true;
                } else if (child instanceof SiteStore && parent == null) {
                    // end of parsing reached
                    mLogger.log(
                            "End of last element </sitecatalog> reached ",
                            LogManager.DEBUG_MESSAGE_LEVEL);
                    return true;

                } else {
                    return false;
                }

            default:
                return false;
        }
    }

    /**
     * Returns the XML schema namespace that a document being parsed conforms to.
     *
     * @return the schema namespace
     */
    public String getSchemaNamespace() {
        return SiteCatalogXMLParser4.SCHEMA_NAMESPACE;
    }

    /**
     * Returns the local path to the XML schema against which to validate.
     *
     * @return path to the schema
     */
    public String getSchemaLocation() {
        // treat URI as File, yes, I know - I need the basename
        File uri = new File(SiteCatalogXMLParser4.SCHEMA_LOCATION);
        // create a pointer to the default local position
        File poolconfig = new File(this.mProps.getSchemaDir(), uri.getName());

        return this.mProps.getPoolSchemaLocation(poolconfig.getAbsolutePath());
    }

    /** @param args */
    public static void main(String[] args) {
        LogManager logger = LogManagerFactory.loadSingletonInstance();
        PegasusProperties properties = PegasusProperties.nonSingletonInstance();
        logger.setLevel(5);
        logger.logEventStart("test.parser", "dax", null);

        List s = new LinkedList();
        s.add("*");
        PegasusBag bag = new PegasusBag();
        bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);
        bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);

        List<String> files = new LinkedList();
        files.add("/lfs1/devel/Pegasus/pegasus/etc/sample-osg-xml4.xml");
        files.add("/lfs1/devel/Pegasus/pegasus/etc/sample-shared-fs-xml4.xml");
        files.add("/lfs1/devel/Pegasus/pegasus/etc/sample-cloud-xml4.xml");

        for (String file : files) {
            SiteCatalogXMLParser4 parser = new SiteCatalogXMLParser4(bag, s);
            System.out.println(" *********Parsing File *********" + file);
            parser.startParser(file);
            SiteStore store = parser.getSiteStore();
            //            System.out.println( store );

            SiteCatalogEntry entry = store.lookup("local");

            SiteDataVisitor visitor = new XML4PrintVisitor();
            StringWriter writer = new StringWriter();
            visitor.initialize(writer);

            try {
                store.accept(visitor);
                System.out.println("Site Catalog is \n" + writer.toString());
            } catch (IOException ex) {
                Logger.getLogger(SiteCatalogXMLParser4.class.getName()).log(Level.SEVERE, null, ex);
            }

            System.out.println(" *********Parsing Done *********");
        }

        //        System.out.println( Directory.TYPE.value( "shared-scratch" ));
    }
}
