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

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.common.util.XMLWriter;
import java.util.LinkedHashSet;

/**
 * This class is the container for any File object, either the RC section, or uses
 *
 * @author gmehta
 * @version $Revision$
 */
public class File extends CatalogType {

    /** The linkages that a file can be of */
    public static enum LINK {
        INPUT,
        input,
        OUTPUT,
        output,
        INOUT,
        inout,
        CHECKPOINT,
        checkpoint
    };

    /**
     * Three Transfer modes supported, Transfer this file, don't transfer or stageout as well as
     * optional. Dont mark transfer or absence as a failure
     */
    public static enum TRANSFER {
        TRUE,
        FALSE,
        OPTIONAL
    }
    /** The namespace on a file. This is used for Executables only */
    protected String mNamespace;
    /** The logical name of the file. */
    protected String mName;
    /** The logical version of the file. This is used for executables only. */
    protected String mVersion;
    /*
     * The Linkage of the file. (Input, Output, or INOUT)
     */
    protected LINK mLink;
    /** Is the file optional */
    protected boolean mOptional = false;
    /** Should the file be registered in the replica catalog */
    protected boolean mRegister = true;
    /** Should the file be transferred on generation. */
    protected TRANSFER mTransfer = TRANSFER.TRUE;
    /** Is the file an executable. */
    protected boolean mExecutable = false;

    /** File size of the file no units required */
    protected String mSize = "";

    /**
     * Create new file object
     *
     * @param name The name of the file
     */
    public File(String name) {
        mName = name;
    }

    /**
     * Create new file object
     *
     * @param name The name of the file
     * @param link The linkage of the file
     */
    public File(String name, LINK link) {
        mName = name;
        mLink = link;
    }

    /**
     * Copy constructor
     *
     * @param f File
     */
    public File(File f) {
        this(f, f.getLink());
    }

    /**
     * Copy constructor, but change the linkage of the file.
     *
     * @param f File
     * @param link Link
     */
    public File(File f, LINK link) {
        this(f.getNamespace(), f.getName(), f.getVersion(), link);
        this.mOptional = f.getOptional();
        this.mRegister = f.getRegister();
        this.mTransfer = f.getTransfer();
        this.mExecutable = f.getExecutable();
        this.mSize = f.getSize();
        this.mMetadata = new LinkedHashSet<MetaData>(f.mMetadata);
    }

    /**
     * Create new File object
     *
     * @param namespace
     * @param name
     * @param version
     */
    public File(String namespace, String name, String version) {
        this(namespace, name, version, null);
    }

    /**
     * Create a new file object
     *
     * @param namespace The namespace of the file
     * @param name The name of the file
     * @param version The version of the file
     * @param link The linkage of the file.
     */
    public File(String namespace, String name, String version, LINK link) {
        super();
        mNamespace = namespace;
        mName = name;
        mVersion = version;
        mLink = link;
    }

    /**
     * Get the name of the file
     *
     * @return
     */
    public String getName() {
        return mName;
    }

    /**
     * Get the namespace of the file
     *
     * @return
     */
    public String getNamespace() {
        return mNamespace;
    }

    /**
     * Get the version of the file
     *
     * @return
     */
    public String getVersion() {
        return mVersion;
    }

    /**
     * Get the linkage of the file.
     *
     * @return
     */
    public LINK getLink() {
        return mLink;
    }

    /**
     * Set the file linkage
     *
     * @param link
     * @return
     * @see LINK
     */
    public File setLink(LINK link) {
        mLink = link;
        return this;
    }

    /**
     * Set the optional flag on the file. Default is false
     *
     * @param optionalflag
     * @return
     */
    public File setOptional(boolean optionalflag) {
        mOptional = optionalflag;
        return this;
    }

    /**
     * Check the optional flag of the file
     *
     * @return
     */
    public boolean getOptional() {
        return mOptional;
    }

    /**
     * Set the register flag of the file. Default is true
     *
     * @param registerflag
     * @return
     */
    public File setRegister(boolean registerflag) {
        mRegister = registerflag;
        return this;
    }

    /**
     * Get the register flag of this file.
     *
     * @return
     */
    public boolean getRegister() {
        return mRegister;
    }

    /**
     * Set the transfer type of the file
     *
     * @param transferflag
     * @return
     * @see TRANSFER
     */
    public File setTransfer(TRANSFER transferflag) {
        mTransfer = transferflag;
        return this;
    }

    /**
     * Get the transfer type of the file
     *
     * @return
     */
    public TRANSFER getTransfer() {
        return mTransfer;
    }

    /**
     * Mark the file as executable. Default is false
     *
     * @param executable
     * @return
     */
    public File setExecutable(boolean executable) {
        mExecutable = executable;
        return this;
    }

    /**
     * Mark the file as executable. Default is false
     *
     * @return
     */
    public File setExecutable() {
        mExecutable = true;
        return this;
    }

    /**
     * Check if the file is an executable
     *
     * @return
     */
    public boolean getExecutable() {
        return mExecutable;
    }

    /**
     * Set the size of the file.
     *
     * @param size
     * @return
     */
    public File setSize(String size) {
        if (size != null) {
            mSize = size;
        }
        return this;
    }

    /**
     * Return the size of the file
     *
     * @return empty string if no size defined, otherwise returns the size
     */
    public String getSize() {
        return mSize;
    }

    public boolean isFile() {
        return true;
    }

    /**
     * Check if this File is equal to Object o
     *
     * @param o
     * @return
     */
    public boolean equals(Object o) {
        if (o instanceof File) {
            File f = (File) o;
            return Separator.combine(mNamespace, mName, mVersion)
                    .equalsIgnoreCase(Separator.combine(f.mNamespace, f.mName, f.mVersion));
        }
        return false;
    }

    /**
     * HashCode of this File
     *
     * @return
     */
    public int hashCode() {
        return Separator.combine(mNamespace, mName, mVersion).hashCode();
    }

    /**
     * Return a clone of this File
     *
     * @return
     */
    public File clone() {
        File f = new File(mNamespace, mName, mVersion, mLink);
        this.mOptional = f.getOptional();
        this.mRegister = f.getRegister();
        this.mTransfer = f.getTransfer();
        this.mExecutable = f.getExecutable();
        this.mSize = f.getSize();
        this.mMetadata = f.getMetaData();
        return f;
    }

    /**
     * Write the file object
     *
     * @param writer
     */
    public void toXML(XMLWriter writer) {
        toXML(writer, 0, "file");
    }

    /**
     * Write the file object, with indent level N
     *
     * @param writer
     * @param indent
     */
    public void toXML(XMLWriter writer, int indent) {
        toXML(writer, indent, "file");
    }

    /**
     * Write the file object as XML but render it as the elementname
     *
     * @param writer
     * @param indent
     * @param elementname
     */
    public void toXML(XMLWriter writer, int indent, String elementname) {
        if (elementname.equalsIgnoreCase("stdin")) {
            // used in job element
            writer.startElement("stdin", indent);
            writer.writeAttribute("name", mName);
            writer.endElement();
        } else if (elementname.equalsIgnoreCase("stdout")) {
            // used in job element
            writer.startElement("stdout", indent);
            writer.writeAttribute("name", mName);
            writer.endElement();
        } else if (elementname.equalsIgnoreCase("stderr")) {
            // used in job element
            writer.startElement("stderr", indent);
            writer.writeAttribute("name", mName);
            writer.endElement();
        } else if (elementname.equalsIgnoreCase("argument")) {
            // used in job's argument element
            writer.startElement("file", indent);
            writer.writeAttribute("name", mName);
            writer.noLine();
            writer.endElement();
        } else if (elementname.equalsIgnoreCase("uses")) {
            // used by job, dax, dag and transformation elements
            writer.startElement("uses", indent);
            if (mNamespace != null && !mNamespace.isEmpty()) {
                writer.writeAttribute("namespace", mNamespace);
            }
            writer.writeAttribute("name", mName);
            if (mVersion != null && !mVersion.isEmpty()) {
                writer.writeAttribute("version", mVersion);
            }
            if (mLink != null) {
                writer.writeAttribute("link", mLink.toString().toLowerCase());

                // transfer flag for input files should be ignored
                if (!(mLink == LINK.INPUT || mLink == LINK.input)) {
                    writer.writeAttribute("transfer", mTransfer.toString().toLowerCase());
                    writer.writeAttribute("register", Boolean.toString(mRegister));
                }
            }
            if (mOptional) {
                writer.writeAttribute("optional", "true");
            }

            if (mExecutable) {
                writer.writeAttribute("executable", "true");
            }
            if (mSize != null && !mSize.isEmpty()) {
                writer.writeAttribute("size", mSize);
            }

            if (mMetadata.isEmpty()) {
                writer.endElement();
            } else {
                // call CatalogType's writer method to generate the profile, metadata and pfn
                // elements
                super.toXML(writer, indent);
                writer.endElement(indent);
            }
        } else if (elementname.equalsIgnoreCase("file")) {
            // Used by the file element at the top of the dax
            if (mPFNs.isEmpty() && mMetadata.isEmpty()) {
                mLogger.log(
                        "The file element for "
                                + mName
                                + " must have atleast 1 pfn or 1 metadata entry. Skipping empty file element",
                        LogManager.WARNING_MESSAGE_LEVEL);
            } else {
                writer.startElement("file", indent);
                writer.writeAttribute("name", mName);

                // call CatalogType's writer method to generate the profile, metadata and pfn
                // elements
                super.toXML(writer, indent);

                writer.endElement(indent);
            }
        }
    }
}
