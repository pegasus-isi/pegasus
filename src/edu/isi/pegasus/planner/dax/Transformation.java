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
 *
 * @author gmehta
 */
public class Transformation {

    protected String mNamespace;
    protected String mName;
    protected String mVersion;
    protected List<File> mUses;

    public Transformation(String name) {
        this("", name, "");
    }

    public Transformation(String namespace, String name, String version) {
        mNamespace = (namespace == null) ? "" : namespace;
        mName = (name == null) ? "" : name;

        mVersion = (version == null) ? "" : null;
        mUses = new LinkedList<File>();
    }

    public String getName() {
        return mName;
    }

    public String getNamespace() {
        return mNamespace;
    }

    public String getVersion() {
        return mVersion;
    }

    public Transformation uses(File file) {
        mUses.add(file);
        return this;
    }

    public Transformation uses(List<File> files) {
        mUses.addAll(files);
        return this;
    }

    public List<File> getUses() {
        return Collections.unmodifiableList(mUses);
    }

    public void toXML(XMLWriter writer) {
        toXML(writer, 0);
    }

    public void toXML(XMLWriter writer, int indent) {
        writer.startElement("transformation", indent);
        if (mNamespace != null && !mNamespace.isEmpty()) {
            writer.writeAttribute("namespace", mNamespace);
        }
        writer.writeAttribute("name", mName);
        if (mVersion != null && !mVersion.isEmpty()) {
            writer.writeAttribute("version", mVersion);
        }
        for (File f : mUses) {
            f.toXML(writer, indent + 1, "uses");
        }
        writer.endElement(indent);
    }
}
