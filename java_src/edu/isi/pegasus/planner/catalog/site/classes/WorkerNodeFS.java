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

package edu.isi.pegasus.planner.catalog.site.classes;

import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.classes.Profile;
import java.io.IOException;
import java.io.Writer;

/**
 * This data class describes the WorkerNode Filesystem layout.
 *
 * @version $Revision$
 * @author Karan Vahi
 */
public class WorkerNodeFS extends AbstractSiteData {

    /** The scratch area on the head node. */
    private WorkerNodeScratch mScratch;

    /** The storage area on the head node. */
    private WorkerNodeStorage mStorage;

    /** The profiles associated with the headnode filesystem. */
    private Profiles mProfiles;

    /** The default constructor. */
    public WorkerNodeFS() {
        mScratch = new WorkerNodeScratch();
        mStorage = new WorkerNodeStorage();
        mProfiles = new Profiles();
    }

    /**
     * The overloaded constructor.
     *
     * @param scratch the scratch area.
     * @param storage the storage area.
     */
    public WorkerNodeFS(WorkerNodeScratch scratch, WorkerNodeStorage storage) {
        setScratch(scratch);
        setStorage(storage);
        mProfiles = new Profiles();
    }

    /**
     * Sets the scratch area on the head node.
     *
     * @param scratch the scratch area.
     */
    public void setScratch(WorkerNodeScratch scratch) {
        mScratch = scratch;
    }

    /**
     * Returns the scratch area on the head node.
     *
     * @return the scratch area.
     */
    public WorkerNodeScratch getScratch() {
        return this.mScratch;
    }

    /**
     * Sets the storage area on the head node.
     *
     * @param storage the storage area.
     */
    public void setStorage(WorkerNodeStorage storage) {
        mStorage = storage;
    }

    /**
     * Returns the storage area on the head node.
     *
     * @return the storage area.
     */
    public WorkerNodeStorage getStorage() {
        return this.mStorage;
    }

    /**
     * Returns the profiles associated with the file server.
     *
     * @return the profiles.
     */
    public Profiles getProfiles() {
        return this.mProfiles;
    }

    /**
     * Sets the profiles associated with the file server.
     *
     * @param profiles the profiles.
     */
    public void setProfiles(Profiles profiles) {
        mProfiles = profiles;
    }

    /**
     * Adds a profile.
     *
     * @param p the profile to be added
     */
    public void addProfile(Profile p) {
        // retrieve the appropriate namespace and then add
        mProfiles.addProfile(p);
    }

    /**
     * Writes out the xml description of the object.
     *
     * @param writer is a Writer opened and ready for writing. This can also be a StringWriter for
     *     efficient output.
     * @param indent the indent to be used.
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML(Writer writer, String indent) throws IOException {
        String newLine = System.getProperty("line.separator", "\r\n");
        String newIndent = indent + "\t";

        // write out the  xml element
        writer.write(indent);
        writer.write("<worker-fs>");
        writer.write(newLine);

        this.getScratch().toXML(writer, newIndent);
        this.getStorage().toXML(writer, newIndent);
        this.mProfiles.toXML(writer, newIndent);

        writer.write(indent);
        writer.write("</worker-fs>");
        writer.write(newLine);
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone() {
        WorkerNodeFS obj;
        try {
            obj = (WorkerNodeFS) super.clone();
            obj.setScratch((WorkerNodeScratch) this.getScratch().clone());
            obj.setStorage((WorkerNodeStorage) this.getStorage().clone());
            obj.setProfiles((Profiles) this.mProfiles.clone());

        } catch (CloneNotSupportedException e) {
            // somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException(
                    "Clone not implemented in the base class of " + this.getClass().getName(), e);
        }
        return obj;
    }

    /**
     * Accepts the visitor and calls visit method on the visitor accordingly
     *
     * @param visitor
     */
    public void accept(SiteDataVisitor visitor) throws IOException {

        visitor.visit(this);

        this.getScratch().accept(visitor);
        this.getStorage().accept(visitor);

        // profiles are handled in the depart method
        visitor.depart(this);
    }
}
