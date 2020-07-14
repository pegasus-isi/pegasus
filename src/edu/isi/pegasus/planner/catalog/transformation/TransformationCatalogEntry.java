/**
 * Copyright 2007-2008 University Of Southern California
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
package edu.isi.pegasus.planner.catalog.transformation;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import edu.isi.pegasus.common.util.ProfileParser;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.catalog.classes.CatalogEntry;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.classes.VDSSysInfo2NMI;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.catalog.transformation.classes.NMI2VDSSysInfo;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationCatalogKeywords;
import edu.isi.pegasus.planner.catalog.transformation.classes.VDSSysInfo;
import edu.isi.pegasus.planner.classes.CompoundTransformation;
import edu.isi.pegasus.planner.classes.Notifications;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusJsonSerializer;
import edu.isi.pegasus.planner.dax.Invoke;
import edu.isi.pegasus.planner.namespace.ENV;
import edu.isi.pegasus.planner.namespace.Metadata;
import edu.isi.pegasus.planner.namespace.Namespace;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

/**
 * An object of this class corresponds to a tuple in the Transformation Catalog.
 *
 * @author Gaurang Mehta @$Revision$
 * @author Karan Vahi
 */
@JsonSerialize(using = TransformationCatalogEntry.JsonSerializer.class)
public class TransformationCatalogEntry implements CatalogEntry {

    /** The logical mNamespace of the transformation */
    private String mNamespace;

    /** The mVersion of the transformation. */
    private String mVersion;

    /** The logical mName of the transformation. */
    private String mName;

    /** The Id of the resource on which the transformation is installed. */
    private String mResourceID;

    /** The physical path on the resource for a particular arch, os and type. */
    private String mPFN;

    /** The profiles associated with the site. */
    private Profiles mProfiles;

    /** The System Info for the transformation. */
    private SysInfo mSysInfo;

    /** The type of transformation. Takes one of the predefined enumerated type TCType. */
    private TCType type = TCType.INSTALLED;

    /** All the notifications associated with the job */
    private Notifications mNotifications;

    /** A reference to the container to use to launch the transformation */
    private Container mContainer;

    /** Boolean flag to indicate whether to bypass staging in the executable via the staging site */
    private boolean mBypassStaging;

    /**
     * if a compound transformation then this is the reference to the container that has all the
     * dependent transformations.
     */
    private CompoundTransformation mCompoundTX;

    /** The basic constructor */
    public TransformationCatalogEntry() {
        mNamespace = null;
        mName = null;
        mVersion = null;
        mResourceID = null;
        mPFN = null;
        mProfiles = null;
        //        sysinfo = null;
        mSysInfo = new SysInfo();
        mNotifications = new Notifications();
        mContainer = null;
        mCompoundTX = null;
        mBypassStaging = false;
    }

    /**
     * Optimized Constructor
     *
     * @param namespace String
     * @param name String
     * @param version String
     */
    public TransformationCatalogEntry(String namespace, String name, String version) {
        this();
        this.mNamespace = namespace;
        this.mVersion = version;
        this.mName = name;
    }

    /**
     * Optimized Constructor
     *
     * @param namespace String
     * @param name String
     * @param version String
     * @param resourceID String
     * @param physicalname String
     * @param type TCType
     * @param profiles List
     * @param sysinfo VDSSysInfo
     */
    public TransformationCatalogEntry(
            String namespace,
            String name,
            String version,
            String resourceid,
            String physicalname,
            TCType type,
            List profiles,
            VDSSysInfo sysinfo) {

        this(namespace, name, version);
        this.mResourceID = resourceid;
        this.mPFN = physicalname;
        if (profiles != null) {
            this.mProfiles = new Profiles();
            this.mProfiles.addProfiles(profiles);
        }

        //       this.sysinfo = sysinfo;
        mSysInfo = VDSSysInfo2NMI.vdsSysInfo2NMI(sysinfo);

        this.type = type;
    }

    /**
     * Overloaded constructor.
     *
     * @param namespace the namespace
     * @param name the name
     * @param version the version
     * @param resourceID the site with which entry is associated
     * @param physicalname the pfn
     * @param type the type
     * @param profiles the profiles passed
     * @param sysinfo the SystemInformation
     */
    private TransformationCatalogEntry(
            String namespace,
            String name,
            String version,
            String resourceID,
            String physicalname,
            TCType type,
            Profiles profiles,
            SysInfo sysinfo) {
        this();
        this.mNamespace = namespace;
        this.mVersion = version;
        this.mName = name;
        this.mResourceID = resourceID;
        this.mPFN = physicalname;
        this.mProfiles = profiles;
        this.mNotifications = new Notifications();

        //       this.sysinfo = sysinfo;
        mSysInfo = sysinfo;

        this.type = type;
    }

    /** Sets the bypass flag denoting the file should be bypassed */
    public void setForBypassStaging() {
        mBypassStaging = true;
    }

    /**
     * Sets the bypass flag denoting the file should be bypassed
     *
     * @param value the boolean value to which the flag should be set to.
     */
    public void setForBypassStaging(boolean value) {
        mBypassStaging = value;
    }

    /**
     * Returns whether file should be attempted for bypassing of input file staging.
     *
     * @return true denoting the file can be cleaned up.
     */
    public boolean bypassStaging() {
        return mBypassStaging;
    }

    /**
     * creates a new instance of this object and returns you it. A shallow clone. TO DO : Gaurang
     * correct the clone method.
     *
     * @return Object
     */
    public Object clone() {
        TransformationCatalogEntry entry =
                new TransformationCatalogEntry(
                        mNamespace,
                        mName,
                        mVersion,
                        mResourceID,
                        mPFN,
                        type,
                        mProfiles,
                        this.getSysInfo());
        entry.setForBypassStaging(this.bypassStaging());
        entry.addNotifications(this.getNotifications());
        entry.setContainer(this.mContainer == null ? null : (Container) mContainer.clone());
        if (this.mCompoundTX != null) {
            for (PegasusFile pf : this.mCompoundTX.getDependantFiles()) {
                entry.addDependantTransformation(pf);
            }
        }
        return entry;
    }

    /**
     * gets the String mVersion of the data class
     *
     * @return String
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("\n ")
                .append("\n Logical Namespace : ")
                .append(this.mNamespace)
                .append("\n Logical Name      : ")
                .append(this.mName)
                .append("\n Version           : ")
                .append(this.mVersion)
                .append("\n Resource Id       : ")
                .append(this.mResourceID)
                .append("\n Physical Name     : ")
                .append(this.mPFN)
                .append("\n SysInfo           : ")
                .append(this.getSysInfo())
                .append("\n TYPE              : ")
                .append(((this.type == null) ? "" : type.toString()));

        if (mProfiles != null) {
            sb.append("\n Profiles : ");
            sb.append(mProfiles);
        }

        sb.append("\n Notifications: ").append(this.mNotifications);
        sb.append("\n Container    : ").append(this.mContainer);
        sb.append("\n Compound Tx  : ").append(this.mCompoundTX);

        return sb.toString();
    }

    /**
     * Prints out a TC file format String.
     *
     * @return String
     */
    public String toTCString() {
        String st =
                this.getResourceId()
                        + "\t"
                        + this.getLogicalTransformation()
                        + "\t"
                        + this.getPhysicalTransformation()
                        + "\t"
                        + this.getType()
                        + "\t"
                        + this.getSysInfo()
                        + "\t";
        if (mProfiles != null) {
            st += ProfileParser.combine(mProfiles);
        } else {
            st += "NULL";
        }
        return st;
    }

    /**
     * Returns an xml output of the contents of the data class.
     *
     * @return String
     */
    public String toXML() {
        String xml =
                "\t\t<pfn physicalName=\""
                        + this.getPhysicalTransformation()
                        + "\""
                        + " siteid=\""
                        + this.getResourceId()
                        + "\""
                        + " type=\""
                        + this.getType()
                        + "\""
                        + " sysinfo=\""
                        + this.getVDSSysInfo()
                        + "\"";
        if (this.mProfiles != null) {
            try {
                xml += " >\n";
                xml += "\t\t\t" + mProfiles.toXML() + "\n";
                xml += "\t\t</pfn>\n";
            } catch (IOException ex) {
                throw new RuntimeException("Error while XML conversion of profiles ", ex);
            }
        } else {
            xml += " />\n";
        }

        return xml;
    }

    /**
     * Set the container to use.
     *
     * @param container
     */
    public void setContainer(Container container) {
        this.mContainer = container;
    }

    /**
     * Set the Compound Transformation for the entry
     *
     * @param compoundTX
     */
    private void setCompoundTransformation(CompoundTransformation compoundTX) {
        this.mCompoundTX = compoundTX;
    }

    /**
     * Adds a dependant transformation
     *
     * @param transformationFQDN
     */
    public void addDependantTransformation(String transformationFQDN) {

        PegasusFile pf = new PegasusFile(transformationFQDN);
        pf.setType(PegasusFile.EXECUTABLE_FILE);
        this.addDependantTransformation(pf);
    }

    /**
     * Adds a dependant transformation
     *
     * @param pf pegasus file object containing the tx
     */
    public void addDependantTransformation(PegasusFile pf) {
        if (this.mCompoundTX == null) {
            this.mCompoundTX =
                    new CompoundTransformation(
                            this.getLogicalNamespace(),
                            this.getLogicalName(),
                            this.getLogicalVersion());
        }
        this.mCompoundTX.addDependantFile(pf);
    }

    /**
     * Returns dependent transformations
     *
     * @return
     */
    public List<PegasusFile> getDependantFiles() {
        if (this.mCompoundTX == null) {
            return new LinkedList();
        }
        return this.mCompoundTX.getDependantFiles();
    }

    /**
     * Merge the container and TC profiles.
     *
     * @param cont
     */
    public void incorporateContainerProfiles(Container cont) {
        // PM-1214 all  ENV profiles have to be merged
        // with the tranformation profile overriding the container
        // but carried forward with the container object.
        ENV containerENVProfiles = (ENV) cont.getAllProfiles().get(Profiles.NAMESPACES.env);

        // merge only if there any profiles associated with the entry
        if (this.mProfiles != null) {
            ENV txENVProfiles = (ENV) this.mProfiles.get(Profiles.NAMESPACES.env);
            containerENVProfiles.merge(txENVProfiles);
            // container object has all the env profiles
            // reset from the Transformation Catalog Entry object
            txENVProfiles.reset();
        }

        // PM-1626 merge lany pegasus profiles from container into the
        // transformation catalog entry object with the TC having a higher precedence
        Pegasus containerPegasusProfiles =
                (Pegasus) cont.getAllProfiles().get(Profiles.NAMESPACES.pegasus);
        Pegasus txPegasusProfiles =
                (Pegasus) this.getProfilesNamepsace(Profiles.NAMESPACES.pegasus);
        if (containerPegasusProfiles != null) {
            for (Iterator<String> it = containerPegasusProfiles.getProfileKeyIterator();
                    it.hasNext(); ) {
                String key = it.next();
                if (txPegasusProfiles == null || !txPegasusProfiles.containsKey(key)) {
                    // only add container profiles into tc entry if key does not
                    // exist beforehand
                    this.addProfile(
                            new Profile(
                                    "pegasus", key, containerPegasusProfiles.getStringValue(key)));
                }
            }
        }
    }

    /**
     * Set the logical transformation with a fully qualified tranformation String of the format
     * NS::NAME:Ver
     *
     * @param logicaltransformation String
     */
    public void setLogicalTransformation(String logicaltransformation) {
        String[] ltr;
        ltr = splitLFN(logicaltransformation);
        this.mNamespace = ltr[0];
        this.mName = ltr[1];
        this.mVersion = ltr[2];
    }

    /**
     * Set the logical transformation by providing the mNamespace, mName and mVersion as seperate
     * strings.
     *
     * @param namespace
     * @param name
     * @param version
     */
    public void setLogicalTransformation(String namespace, String name, String version) {
        this.mNamespace = namespace;
        this.mName = name;
        this.mVersion = version;
    }

    /**
     * Set the logical mNamespace of the transformation.
     *
     * @param namespace String
     */
    public void setLogicalNamespace(String namespace) {
        this.mNamespace = namespace;
    }

    /**
     * Set the logical mName of the transformation.
     *
     * @param name String
     */
    public void setLogicalName(String name) {
        this.mName = name;
    }

    /**
     * Set the logical mVersion of the transformation.
     *
     * @param version String
     */
    public void setLogicalVersion(String version) {
        this.mVersion = version;
    }

    /**
     * Set the mResourceID where the transformation is available.
     *
     * @param resourceID String
     */
    public void setResourceId(String resourceid) {
        this.mResourceID = resourceid;
    }

    /**
     * Set the type of the transformation.
     *
     * @param type TCType
     */
    public void setType(TCType type) {
        this.type = (type == null) ? TCType.INSTALLED : type;
    }

    /**
     * Set the physical location of the transformation.
     *
     * @param pfn String
     */
    public void setPhysicalTransformation(String pfn) {
        this.mPFN = pfn;
    }

    /**
     * Sets the system information for the entry.
     *
     * @param sysinfo the System information
     */
    public void setSysInfo(SysInfo sysinfo) {
        this.mSysInfo = sysinfo;
    }

    /**
     * Set the System Information associated with the transformation.
     *
     * @param sysinfo VDSSysInfo
     */
    public void setVDSSysInfo(VDSSysInfo sysinfo) {
        this.mSysInfo = (sysinfo == null) ? new SysInfo() : VDSSysInfo2NMI.vdsSysInfo2NMI(sysinfo);
    }

    /**
     * Adds a Invoke object correpsonding to a notification.
     *
     * @param invoke the invoke object containing the notification
     */
    public void addNotification(Invoke invoke) {
        this.mNotifications.add(invoke);
    }

    /**
     * Adds all the notifications passed to the underlying container.
     *
     * @param invokes the notifications to be added
     */
    public void addNotifications(Notifications invokes) {
        this.mNotifications.addAll(invokes);
    }

    /**
     * Returns a collection of all the notifications that need to be done for a particular condition
     *
     * @param when the condition
     * @return
     */
    public Collection<Invoke> getNotifications(Invoke.WHEN when) {
        return this.mNotifications.getNotifications(when);
    }

    /**
     * Returns all the notifications associated with the job.
     *
     * @return the notifications
     */
    public Notifications getNotifications() {
        return this.mNotifications;
    }

    /**
     * Allows you to add one profile at a time to the transformation.
     *
     * @param profiles profiles to be added.
     */
    public void addProfiles(Profiles profiles) {
        if (profiles != null) {
            if (this.mProfiles == null) {
                this.mProfiles = new Profiles();
            }
            this.mProfiles.addProfilesDirectly(profiles);
        }
    }

    /**
     * Allows you to add one profile at a time to the transformation.
     *
     * @param profile Profile A single profile consisting of mNamespace, key and value
     */
    public void addProfile(Profile profile) {
        if (profile != null) {
            if (this.mProfiles == null) {
                this.mProfiles = new Profiles();
            }
            // PM-826 allow multiple profiles with same key
            if (profile.getProfileNamespace().equalsIgnoreCase(Pegasus.NAMESPACE_NAME)) {
                this.mProfiles.addProfile(profile);
            } else {
                this.mProfiles.addProfileDirectly(profile);
            }
        }
    }

    /**
     * Allows you to add multiple profiles to the transformation.
     *
     * @param profiles List of Profile objects containing the profile information.
     */
    public void addProfiles(List profiles) {
        if (profiles != null) {
            if (this.mProfiles == null) {
                this.mProfiles = new Profiles();
            }
            this.mProfiles.addProfilesDirectly(profiles);
        }
    }

    /**
     * Return the container to be used to launch the executable
     *
     * @return
     */
    public Container getContainer() {
        return mContainer;
    }

    /**
     * Gets the Fully Qualified Transformation mName in the format NS::Name:Ver.
     *
     * @return String
     */
    public String getLogicalTransformation() {
        return joinLFN(mNamespace, mName, mVersion);
    }

    /**
     * Returns the Namespace associated with the logical transformation.
     *
     * @return String Returns null if no mNamespace associated with the transformation.
     */
    public String getLogicalNamespace() {
        return this.mNamespace;
    }

    /**
     * Returns the Name of the logical transformation.
     *
     * @return String
     */
    public String getLogicalName() {
        return this.mName;
    }

    /**
     * Returns the mVersion of the logical transformation.
     *
     * @return String Returns null if no mVersion assocaited with the transformation.
     */
    public String getLogicalVersion() {
        return this.mVersion;
    }

    /**
     * Returns the resource where the transformation is located.
     *
     * @return String
     */
    public String getResourceId() {
        return this.mResourceID;
    }

    /**
     * Returns the type of the transformation.
     *
     * @return TCType
     */
    public TCType getType() {
        return this.type;
    }

    /**
     * Returns the physical location of the transformation.
     *
     * @return String
     */
    public String getPhysicalTransformation() {
        return this.mPFN;
    }

    /**
     * Returns the System Information associated with the transformation.
     *
     * @return SysInfo
     */
    public SysInfo getSysInfo() {
        return mSysInfo;
    }

    /**
     * Returns the System Information in the old VDS format associated with the transformation.
     *
     * @return VDSSysInfo
     */
    public VDSSysInfo getVDSSysInfo() {
        return NMI2VDSSysInfo.nmiToVDSSysInfo(mSysInfo);
    }

    /**
     * Returns the list of profiles associated with the transformation.
     *
     * @return List Returns null if no profiles associated.
     */
    public List getProfiles() {
        return (this.mProfiles == null) ? null : this.mProfiles.getProfiles();
    }

    /**
     * Returns the list of all profiles associated with the transformation as a Profiles object.
     *
     * @return List Returns null if no profiles associated.
     */
    public Profiles getAllProfiles() {
        return this.mProfiles;
    }

    /**
     * Returns the profiles for a particular Namespace.
     *
     * @param namespace String The mNamespace of the profile
     * @return List List of Profile objects. returns null if none are found.
     * @deprecated use getProfiles(Profiles.NAMESPACES)
     */
    public List getProfiles(String namespace) {
        return (this.mProfiles == null) ? null : mProfiles.getProfiles(namespace);
    }

    /**
     * Returns the profiles for a particular namespace.
     *
     * @param namespace namespace of the profile
     * @return List List of Profile objects. returns null if none are found.
     */
    public List getProfiles(Profiles.NAMESPACES namespace) {
        return (this.mProfiles == null) ? null : mProfiles.getProfiles(namespace);
    }

    /**
     * Returns the profiles for a particular namespace.
     *
     * @param namespace namespace of the profile
     * @return namespace object
     */
    public Namespace getProfilesNamepsace(Profiles.NAMESPACES namespace) {
        return (this.mProfiles == null) ? null : mProfiles.get(namespace);
    }

    /**
     * Returns a boolean indicating whether there is a checksum associated with the executable or
     * not.
     *
     * @return
     */
    public boolean hasCheckSum() {
        boolean has = false;
        Metadata m = (Metadata) this.getProfilesNamepsace(Profiles.NAMESPACES.metadata);
        if (m != null) {
            has = m.containsKey(Metadata.CHECKSUM_VALUE_KEY);
        }
        return has;
    }

    /**
     * Joins the 3 components into a fully qualified logical mName of the format NS::NAME:VER
     *
     * @param mNamespace String
     * @param mName String
     * @param mVersion String
     * @return String
     */
    private static String joinLFN(String namespace, String name, String version) {
        return Separator.combine(namespace, name, version);
    }

    /**
     * Splits the full qualified logical transformation into its components.
     *
     * @param logicaltransformation String
     * @return String[]
     */
    private static String[] splitLFN(String logicaltransformation) {
        return Separator.split(logicaltransformation);
    }

    /**
     * Matches two Mount Point objects. The primary key in this case is the source directory and the
     * destination directory
     *
     * @return true if directories combo match
     */
    public boolean equals(Object obj) {
        // null check
        if (obj == null) {
            return false;
        }

        // see if type of objects match
        if (!(obj instanceof TransformationCatalogEntry)) {
            return false;
        }
        TransformationCatalogEntry entry = (TransformationCatalogEntry) obj;
        return this.toTCString().equalsIgnoreCase(entry.toTCString());
    }

    /**
     * Compares two catalog entries for equality.
     *
     * @param entry is the entry to compare with
     * @return true if the entries match, false otherwise
     */
    public boolean equals(TransformationCatalogEntry entry) {
        return this.toTCString().equalsIgnoreCase(entry.toTCString());
    }

    /**
     * Custom serializer for YAML representation of Transformation Catalog Entry
     *
     * @author Karan Vahi
     */
    static class JsonSerializer extends PegasusJsonSerializer<TransformationCatalogEntry> {

        public JsonSerializer() {}

        /**
         * Serializes contents into YAML representation
         *
         * @param entry
         * @param gen
         * @param sp
         * @throws IOException
         */
        public void serialize(
                TransformationCatalogEntry entry, JsonGenerator gen, SerializerProvider sp)
                throws IOException {
            gen.writeStartObject();

            writeStringField(
                    gen,
                    TransformationCatalogKeywords.NAMESPACE.getReservedName(),
                    entry.getLogicalNamespace());
            writeStringField(
                    gen,
                    TransformationCatalogKeywords.NAME.getReservedName(),
                    entry.getLogicalName());
            writeStringField(
                    gen,
                    TransformationCatalogKeywords.VERSION.getReservedName(),
                    entry.getLogicalVersion());

            if (!entry.getNotifications().isEmpty()) {
                gen.writeFieldName(TransformationCatalogKeywords.HOOKS.getReservedName());
                gen.writeObject(entry.getNotifications());
            }

            if (entry.getResourceId() == null) {
                // write out profiles if specified
                // we normally do them in the sites array
                // for serialization profiles are associated at site level
                Profiles profiles = entry.getAllProfiles();
                if (profiles != null) {
                    this.serializeProfiles(gen, profiles);
                }
            } else {
                // we only have one site associated
                gen.writeArrayFieldStart(TransformationCatalogKeywords.SITES.getReservedName());

                gen.writeStartObject();
                writeStringField(
                        gen,
                        TransformationCatalogKeywords.NAME.getReservedName(),
                        entry.getResourceId());

                writeStringField(
                        gen,
                        TransformationCatalogKeywords.TYPE.getReservedName(),
                        entry.getType().toString().toLowerCase());

                writeStringField(
                        gen,
                        TransformationCatalogKeywords.SITE_PFN.getReservedName(),
                        entry.getPhysicalTransformation());

                gen.writeBooleanField(
                        TransformationCatalogKeywords.BYPASS.getReservedName(),
                        entry.bypassStaging());

                SysInfo sys = entry.getSysInfo();
                writeStringField(
                        gen,
                        TransformationCatalogKeywords.SITE_ARCHITECTURE.getReservedName(),
                        sys.getArchitecture().toString());
                writeStringField(
                        gen,
                        TransformationCatalogKeywords.SITE_OS_TYPE.getReservedName(),
                        sys.getOS().toString());
                writeStringField(
                        gen,
                        TransformationCatalogKeywords.SITE_OS_RELEASE.getReservedName(),
                        sys.getOSRelease());
                writeStringField(
                        gen,
                        TransformationCatalogKeywords.SITE_OS_VERSION.getReservedName(),
                        sys.getOSVersion());

                // for serialization profiles are associated at site level
                Profiles profiles = entry.getAllProfiles();
                if (profiles != null) {
                    this.serializeProfiles(gen, profiles);
                }

                // write out container reference if set
                Container c = entry.getContainer();
                if (c != null) {
                    writeStringField(
                            gen,
                            TransformationCatalogKeywords.SITE_CONTAINER_NAME.getReservedName(),
                            c.getName());
                }

                gen.writeEndObject();
                gen.writeEndArray();
            }

            gen.writeEndObject();
        }

        /**
         * Special handling for profiles serialization to ensure that metadata and checksum
         * information is serialized at the same level as profiles and not nested inside .
         *
         * @param gen
         * @param profiles
         * @throws IOException
         */
        private void serializeProfiles(JsonGenerator gen, Profiles profiles) throws IOException {
            // PM-1617 metadata and checksum are at the profiles level , not nested in there
            // remove the metadata and add back after serializing profiles
            Metadata m = (Metadata) profiles.remove(Profiles.NAMESPACES.metadata);

            if (!profiles.isEmpty()) {
                gen.writeFieldName(TransformationCatalogKeywords.PROFILES.getReservedName());
                gen.writeObject(profiles);
            }
            gen.writeObject(m);
            profiles.set(m);
        }
    }
}
