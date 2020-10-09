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
package edu.isi.pegasus.planner.dax;

import edu.isi.pegasus.common.util.XMLWriter;
import java.util.LinkedList;
import java.util.List;

/**
 * This Object is used to create a complex Transformation. A complex transformation is one that uses
 * other executables and files
 *
 * @author gmehta
 * @version $Revision$
 */
public class Transformation {

    /** Namespace of the Transformation */
    protected String mNamespace;
    /** Name of the transformation */
    protected String mName;
    /** Version of the transformation */
    protected String mVersion;
    /** List of executable of files used by the transformation */
    protected List<CatalogType> mUses;

    protected List<Invoke> mInvokes;

    /**
     * Create a new Transformation object
     *
     * @param name the name of transformation
     */
    public Transformation(String name) {
        this("", name, "");
    }

    /**
     * Copy Constructor
     *
     * @param t  Transformation to copy from
     */
    public Transformation(Transformation t) {
        this(t.mNamespace, t.mName, t.mVersion);
        this.mUses = new LinkedList<CatalogType>(t.mUses);
        this.mInvokes = new LinkedList<Invoke>(t.mInvokes);
    }

    /**
     * Create a new Transformation Object
     *
     * @param namespace the namespace
     * @param name      the name
     * @param version   the version
     */
    public Transformation(String namespace, String name, String version) {
        mNamespace = (namespace == null) ? "" : namespace;
        mName = (name == null) ? "" : name;

        mVersion = (version == null) ? "" : version;
        mUses = new LinkedList<CatalogType>();
        mInvokes = new LinkedList<Invoke>();
    }

    /**
     * Get the name of the transformation
     *
     * @return the name
     */
    public String getName() {
        return mName;
    }

    /**
     * Get the namespace of the transformation
     *
     * @return the namespace
     */
    public String getNamespace() {
        return mNamespace;
    }

    /**
     * Get the version of the transformation
     *
     * @return the version
     */
    public String getVersion() {
        return mVersion;
    }

    /**
     * Return the list of Notification objects
     *
     * @return List of Invoke objects
     */
    public List<Invoke> getInvoke() {
        return mInvokes;
    }

    /**
     * Return the list of Notification objects (same as getInvoke()
     *
     * @return List of Invoke objects
     */
    public List<Invoke> getNotification() {
        return getInvoke();
    }

    /**
     * Add a Notification for this Transformation
     *
     * @param when   when to invoke the notification
     * @param what   the executable to invoke with the arg string
     * @return Transformation
     */
    public Transformation addInvoke(Invoke.WHEN when, String what) {
        Invoke i = new Invoke(when, what);
        mInvokes.add(i);
        return this;
    }

    /**
     * Add a Notification for this Transformation same as addInvoke()
     *
     * @param when   when to invoke the notification
     * @param what   the executable to invoke with the arg string
     * @return Transformation
     */
    public Transformation addNotification(Invoke.WHEN when, String what) {
        return addInvoke(when, what);
    }

    /**
     * Add a Notification for this Transformation
     *
     * @param invoke  the invoke object containing the invocation to invoke
     * @return Transformation
     */
    public Transformation addInvoke(Invoke invoke) {
        mInvokes.add(invoke.clone());
        return this;
    }

    /**
     * Add a List of Notifications for this Transformation
     *
     * @param invokes    list of notifications to associate with
     * @return Transformation
     */
    public Transformation addInvokes(List<Invoke> invokes) {
        for (Invoke invoke : invokes) {
            this.addInvoke(invoke);
        }
        return this;
    }

    /**
     * Add a List of Notifications for this Transformation. Same as addInvokes()
     *
     * @param invokes list of notifications to associate with
     * @return Transformation
     */
    public Transformation addNotifications(List<Invoke> invokes) {
        return addInvokes(invokes);
    }

    /**
     * Set the file or executable being used by the transformation
     *
     * @param fileorexecutable  file used by executable
     * @return Transformation
     */
    public Transformation uses(CatalogType fileorexecutable) {
        mUses.add(fileorexecutable);
        return this;
    }

    /**
     * Set the List of files and/or executables being used by the transformation
     *
     * @param filesorexecutables list of files or executables used by the transformation
     * @return Transformation
     */
    public Transformation uses(List<CatalogType> filesorexecutables) {
        mUses.addAll(filesorexecutables);
        return this;
    }

    /**
     * Get the List of files and/or executables being used by the transformation
     *
     * @return List of Files and Executables used
     */
    public List<CatalogType> getUses() {
        return mUses;
    }

    /**
     * Returns boolean indicating whether this instance of object is equal to the
     * object being passed
     * 
     * @param obj object being compared
     * 
     * @return  boolean 
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        final Transformation other = (Transformation) obj;
        if ((this.mNamespace == null)
                ? (other.mNamespace != null)
                : !this.mNamespace.equals(other.mNamespace)) {
            return false;
        }
        if ((this.mName == null) ? (other.mName != null) : !this.mName.equals(other.mName)) {
            return false;
        }
        if ((this.mVersion == null)
                ? (other.mVersion != null)
                : !this.mVersion.equals(other.mVersion)) {
            return false;
        }
        return true;
    }

    /**
     * Return the hashcode
     * 
     * @return int 
     */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 47 * hash + (this.mNamespace != null ? this.mNamespace.hashCode() : 0);
        hash = 47 * hash + (this.mName != null ? this.mName.hashCode() : 0);
        hash = 47 * hash + (this.mVersion != null ? this.mVersion.hashCode() : 0);
        return hash;
    }

    /**
     * Return the String version
     * 
     * @return String 
     */
    @Override
    public String toString() {
        return mNamespace + "::" + mName + ":" + mVersion;
    }

    /**
     * Writes out XML representation using the writer
     * 
     * @param writer  the writer 
     */
    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    /**
     * Writes out XML representation using the writer
     * 
     * @param writer  the writer 
     * @param indent  number pf indent spaces
     */
    public void toXML(XMLWriter writer, int indent) {
        if (!mUses.isEmpty()) {
            writer.startElement("transformation", indent);
            if (mNamespace != null && !mNamespace.isEmpty()) {
                writer.writeAttribute("namespace", mNamespace);
            }
            writer.writeAttribute("name", mName);
            if (mVersion != null && !mVersion.isEmpty()) {
                writer.writeAttribute("version", mVersion);
            }
            for (CatalogType c : mUses) {
                if (c.getClass() == File.class) {
                    File f = (File) c;
                    writer.startElement("uses", indent + 1);
                    writer.writeAttribute("name", f.getName());
                    // PM-1316 data files should explicitly have executable set to false
                    // to override the default attribute value for this in the schema for
                    // occurences of uses in the transformation element
                    writer.writeAttribute("executable", "false");
                    writer.endElement();
                } else if (c.getClass() == Executable.class) {
                    Executable e = (Executable) c;
                    writer.startElement("uses", indent + 1);
                    if (e.mNamespace != null && !e.mNamespace.isEmpty()) {
                        writer.writeAttribute("namespace", e.mNamespace);
                    }
                    writer.writeAttribute("name", e.mName);
                    if (e.mVersion != null && !e.mVersion.isEmpty()) {
                        writer.writeAttribute("version", e.mVersion);
                    }
                    writer.writeAttribute("executable", "true");
                    writer.endElement();
                }
            }
            for (Invoke i : mInvokes) {
                i.toXML(writer, indent + 1);
            }
            writer.endElement(indent);
        }
    }
}
