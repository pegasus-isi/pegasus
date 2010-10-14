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

    public MetaData(MetaData m){
        //create a copy
        this(m.getKey(),m.getType(),m.getValue());
    }

    public MetaData(String type, String key){
        mType=type;
        mKey=key;
    }

    public MetaData(String type, String key, String value){
        mType=type;
        mKey=key;
        mValue=value;
    }

    public MetaData clone(MetaData m){
        return new MetaData(m.getKey(),m.getType(),m.getValue());
    }
    
    public MetaData setValue(String value){
        mValue=value;
        return this;
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


