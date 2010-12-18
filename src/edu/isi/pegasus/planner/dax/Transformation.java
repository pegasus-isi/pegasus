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

import java.util.Collections;
import java.util.List;
import java.util.LinkedList;
import edu.isi.pegasus.common.util.XMLWriter;

/**
 * This Object is used to create a complex Transformation.
 * A complex transformation is one that uses other executables and files
 * @author gmehta
 * @version $Revision$
 */
public class Transformation {

    /**
     * Namespace of the Transformation
     */
    protected String mNamespace;
    /**
     * Name of the transformation
     */
    protected String mName;
    /**
     * Version of the transformation
     */
    protected String mVersion;
    /**
     * List of executable of files used by the transformation
     */
    protected List<CatalogType> mUses;

    /**
     * Create a new Transformation object
     * @param name
     */
    public Transformation(String name) {
        this("", name, "");
    }

    /**
     * Create a new Transformation Object
     * @param namespace
     * @param name
     * @param version
     */
    public Transformation(String namespace, String name, String version) {
        mNamespace = (namespace == null) ? "" : namespace;
        mName = (name == null) ? "" : name;

        mVersion = (version == null) ? "" : null;
        mUses = new LinkedList<CatalogType>();
    }

    /**
     * Get the name of the transformation
     * @return
     */
    public String getName() {
        return mName;
    }

    /**
     * Get the namespace of the transformation
     * @return
     */
    public String getNamespace() {
        return mNamespace;
    }

    /**
     * Get the version of the transformation
     * @return
     */
    public String getVersion() {
        return mVersion;
    }

    /**
     * Set the file or executable being used by the transformation
     * @param fileorexecutable
     * @return
     */
    public Transformation uses(CatalogType fileorexecutable) {
        mUses.add(fileorexecutable);
        return this;
    }

    /**
     * Set the List of files and/or executables being used by the transformation
     *
     * @param filesorexecutables
     * @return
     */
    public Transformation uses(List<CatalogType> filesorexecutables) {
        mUses.addAll(filesorexecutables);
        return this;
    }

    /**
     * Get the List of files and/or executables being used by the transformation
     * @return
     */
    public List<CatalogType> getUses() {
        return Collections.unmodifiableList(mUses);
    }

    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

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
            writer.endElement(indent);
        }
    }
}
