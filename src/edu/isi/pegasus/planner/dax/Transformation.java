/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.isi.pegasus.planner.dax;

import java.util.*;
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
        writer.startElement("transformation");
            if(mNamespace!=null && !mNamespace.isEmpty()){
                writer.writeAttribute("namespace",mNamespace);
            }
            writer.writeAttribute("name", mName);
            if(mVersion!=null && !mVersion.isEmpty()){
                writer.writeAttribute("version",mVersion);
            }
            for(File f : mUses){
                f.toXML(writer, "uses");
            }
            writer.endElement();
    }
}
