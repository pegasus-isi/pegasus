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
public class MetaData {
    protected String mKey;
    protected String mType;
    protected String mValue;

    public MetaData(String type, String key, String value){
        mType=type;
        mKey=key;
        mValue=value;
    }

    public String getKey(){
        return mKey;
    }

    public String getType(){
        return mType;
    }

    public String getValue(){
        return mValue;
    }

    public void toXML(XMLWriter writer){
        writer.startElement("metadata").writeAttribute("type",mType).writeAttribute("key", mKey).writeData(mValue).endElement();

    }
}


