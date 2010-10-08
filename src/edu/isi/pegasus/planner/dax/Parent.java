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
public class Parent implements Comparable {

    /**
     * The name of the parent
     */
    private String mName;
    /**
     * The edge label for the parent child relationship. Optional.
     */
    private String mLabel;

    /**
     *
     * @param name
     */
    public Parent(String name) {
        mName = name;
    }

    /**
     *
     * @param name
     * @param label
     */
    public Parent(String name, String label) {
        mName = name;
        mLabel = label;
    }

    /**
     * @return the name of the parent
     */
    public String getName() {
        return mName;
    }

    /**
     * @param name the name of the parent to set
     */
    public void setName(String name) {
        mName = name;
    }

    /**
     * @return the label
     */
    public String getLabel() {
        return mLabel;
    }

    /**
     * @param label the label to set
     */
    public void setLabel(String label) {
        mLabel = label;
    }

    @Override
    public int compareTo(Object o) {
        int cmp = mName.compareTo(((Parent) o).getName());
        return cmp == 0 ? mLabel.compareTo(((Parent) o).getLabel()) : cmp;
    }

    @Override
    public int hashCode() {
        int hashcode;
        if (mLabel == null) {
            hashcode = 0;
        } else {
            hashcode = mLabel.hashCode();
        }
        return 31 * mName.hashCode() + hashcode;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Parent)) {
            return false;
        }
        if (this == o) {
            return true;
        }
        return mName.equals(((Parent) o).getName()) && mLabel.equals(((Parent) o).getLabel());
    }

    @Override
    public String toString() {
        return "(" + mName + ", " + mLabel == null ? "" : mLabel + ')';
    }
    
    public void toXML(XMLWriter writer){
        writer.startElement("parent");
        writer.writeAttribute("ref",mName);
        if(mLabel!=null && !mLabel.isEmpty()){
            writer.writeAttribute("edge-label",mLabel);
        }
        writer.endElement();
        
    }

}
