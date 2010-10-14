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
public class File extends CatalogType{

    public static enum LINK {

        INPUT, OUTPUT, INOUT, NONE
    };

    public static enum TRANSFER {
        TRUE, FALSE, OPTIONAL
    }
    
     protected String mNamespace;
    protected String mName;
    protected String mVersion;

    protected LINK mLink;
    protected boolean mOptional=false;
    protected boolean mRegister=true;
    protected TRANSFER mTransfer=TRANSFER.TRUE;
    protected boolean mExecutable=false;

    public File(String name) {
        mName = name;
    }

    public File (String namespace, String name, String version){
        mNamespace=namespace;
        mName=name;
        mVersion=version;
    }

    public File(String name, LINK link) {
        mName = name;
        mLink = link;
    }

    public File (String namespace, String name, String version,LINK link){
        mNamespace=namespace;
        mName=name;
        mVersion=version;
        mLink=link;
    }


    public String getName() {
        return mName;
    }

    public String getNamespace(){
        return mNamespace;
    }

    public String getVersion(){
        return mVersion;

    }

    public LINK getLink(){
        return mLink;
    }

    public File setLink(LINK link){
        mLink=link;
        return this;
    }

    public File setOptional(boolean optionalflag){
        mOptional=optionalflag;
        return this;
    }

    public boolean getOptional(){
        return mOptional;
        
    }

    public File setRegister(boolean registerflag){
        mRegister=registerflag;
        return this;
    }

    public boolean getRegister(){
        return mRegister;
    }

    public File setTransfer(TRANSFER transferflag){
        mTransfer=transferflag;
        return this;
    }

    public TRANSFER getTransfer(){
        return mTransfer;
    }

    public void toXML(XMLWriter writer){
        toXML(writer,"file");
    }
    public void toXML(XMLWriter writer, String elementname){
        if(elementname.equalsIgnoreCase("stdin")){
            //used in job element
            writer.startElement("stdin");
            writer.writeAttribute("name", mName);
            writer.endElement();
        } else if(elementname.equalsIgnoreCase("stdout")) {
            //used in job element
            writer.startElement("stdout");
            writer.writeAttribute("name", mName);
            writer.endElement();
        }else if(elementname.equalsIgnoreCase("stderr")) {
            //used in job element
            writer.startElement("stderr");
            writer.writeAttribute("name", mName);
            writer.endElement();
        }else if(elementname.equalsIgnoreCase("argument")) {
            //used in job's argument element
            writer.startElement("file");
            writer.writeAttribute("name", mName);
            writer.noLine();
            writer.endElement();
        }else if(elementname.equalsIgnoreCase("uses")) {
            // used by job, dax, dag and transformation elements
            writer.startElement("uses");
            if(mNamespace!=null && !mNamespace.isEmpty()){
                writer.writeAttribute("namespace",mNamespace);
            }
            writer.writeAttribute("name", mName);
            if(mVersion!=null && !mVersion.isEmpty()){
                writer.writeAttribute("version",mVersion);
            }
            if(mLink!=null) {
                writer.writeAttribute("link", mLink.toString().toLowerCase());
            }
            if(mOptional){
                writer.writeAttribute("optional", "true");
            }
            if(mTransfer!=TRANSFER.TRUE){
                writer.writeAttribute("transfer", mTransfer.toString().toLowerCase());
            }
            if(!mRegister){
                writer.writeAttribute("register","false");
            }
            if(mExecutable){
                writer.writeAttribute("executable","true");
            }
            writer.endElement();
        } else if(elementname.equalsIgnoreCase("file")){
            //Used by the file element at the top of the dax
            writer.startElement("file");
            writer.writeAttribute("name", mName);
            if(mLink!=null) {
                writer.writeAttribute("link", mLink.toString().toLowerCase());
            }
            if(mOptional){
                writer.writeAttribute("optional", "true");
            }
            //call CatalogType's writer method to generate the profile, metadata and pfn elements
            super.toXML(writer);

            writer.endElement();
        }

    }


}
