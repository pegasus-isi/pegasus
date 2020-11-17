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
package edu.isi.pegasus.planner.classes;

import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.planner.dax.Invoke;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

/**
 * A data class to contain compound transformations.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class CompoundTransformation {

    /** The namespace of the compound transformation. */
    protected String mNamespace;

    /** The name of the tranformation. */
    protected String mName;

    /** The version */
    protected String mVersion;

    /** The list of dependant executables */
    protected List<PegasusFile> mUses;

    /** All the notifications associated with the job */
    protected Notifications mNotifications;

    /**
     * Constructor
     *
     * @param name of transformation
     */
    public CompoundTransformation(String name) {
        this("", name, "");
    }

    /**
     * Overloaded Constructor
     *
     * @param namespace namespace
     * @param name name
     * @param version version
     */
    public CompoundTransformation(String namespace, String name, String version) {
        mNamespace = (namespace == null) ? "" : namespace;
        mName = (name == null) ? "" : name;

        mVersion = (version == null) ? "" : version;
        mUses = new LinkedList<PegasusFile>();
        mNotifications = new Notifications();
    }

    /**
     * Returns name of compound transformation.
     *
     * @return name
     */
    public String getName() {
        return mName;
    }

    /**
     * Returns the namespace
     *
     * @return namespace
     */
    public String getNamespace() {
        return mNamespace;
    }

    /**
     * Returns the version
     *
     * @return version
     */
    public String getVersion() {
        return mVersion;
    }

    /**
     * Adds a dependant file.
     *
     * @param pf
     */
    public void addDependantFile(PegasusFile pf) {
        this.mUses.add(pf);
    }

    /**
     * Returns the List of dependant files
     *
     * @return List of Dependant Files
     */
    public List<PegasusFile> getDependantFiles() {
        return this.mUses;
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
     * Returns whether two objects are equal or not on the basis of the complete name of the
     * transformation.
     *
     * @param obj the reference object with which to compare.
     * @return true, if the primary keys match, false otherwise.
     */
    public boolean equals(Object obj) {
        // ward against null
        if (obj == null) return false;

        // shortcut
        if (obj == this) return true;

        // compare similar objects only
        if (!(obj instanceof CompoundTransformation)) return false;

        // now we can safely cast
        CompoundTransformation c = (CompoundTransformation) obj;
        return this.getCompleteName().equals(c.getCompleteName());
    }

    /**
     * Calculate a hash code value for the object to support hash tables. The hashcode value is
     * computed only on basis of namespace, name and version fields
     *
     * @return a hash code value for the object.
     */
    public int hashCode() {
        return this.getCompleteName().hashCode();
    }

    /**
     * Returns the complete name for the transformation.
     *
     * @return the complete name
     */
    public String getCompleteName() {
        return Separator.combine(mNamespace, mName, mVersion);
    }

    /**
     * Converts object to String
     *
     * @return the textual description
     */
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("Transformation -> ").append(this.getCompleteName()).append("\n");

        for (PegasusFile pf : this.getDependantFiles()) {
            sb.append("\t ");
            sb.append(pf.getType() == PegasusFile.DATA_FILE ? "data" : "executable")
                    .append(" -> ")
                    .append(pf)
                    .append("\n");
        }
        sb.append("Notifications -> ").append("\n").append(this.getNotifications());
        return sb.toString();
    }
}
