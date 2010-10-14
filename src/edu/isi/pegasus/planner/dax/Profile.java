/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.planner.dax;
import edu.isi.pegasus.common.util.XMLWriter;

/**
 *
 * @author gmehta
 */
public class Profile {

    public static enum NAMESPACE{
        condor,pegasus,dagman,globus,hints,selector,stat,env
    }
    protected String mNamespace;
    protected String mKey;
    protected String mValue;

    public Profile(String namespace, String key, String value){
        mNamespace=namespace;
        mKey=key;
        mValue=value;
    }

    public Profile(NAMESPACE namespace, String key, String value){
        mNamespace=namespace.toString();
        mKey=key;
        mValue=value;
    }
    public String getKey(){
        return mKey;
    }

    public String getNameSpace(){
        return mNamespace;
    }

    public String getValue(){
        return mValue;
    }

    public void toXML(XMLWriter writer){
        writer.startElement("profile");
        writer.writeAttribute("namespace", mNamespace);
        writer.writeAttribute("key", mKey);
        writer.writeData(mValue);
        writer.endElement();
 
    }
}


