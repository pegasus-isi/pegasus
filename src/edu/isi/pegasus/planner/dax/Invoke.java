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
 public  class Invoke {

        public static enum WHEN {

            never, start, on_success, on_error, at_end, all
        };
        protected WHEN mWhen;
        protected String mWhat;

        
        public Invoke(Invoke i){
            this(WHEN.valueOf(i.getWhen()),i.getWhat());
        }
        public Invoke(WHEN when) {
            mWhen = when;
        }

        public Invoke(WHEN when, String what) {
            mWhen = when;
            mWhat = what;

        }

        public String getWhen() {
            return mWhen.toString();
        }

        public Invoke setWhen(WHEN when) {
            mWhen = when;
            return this;
        }

        public String getWhat() {
            return mWhat;
        }

        public Invoke setWhat(String what) {
            mWhat = what;
            return this;
        }

        public Invoke clone(Invoke i){
            return new Invoke(WHEN.valueOf(i.getWhen()),i.getWhat());
        }

    public void toXML(XMLWriter writer) {
        writer.startElement("invoke").writeAttribute("when", mWhen.toString().toLowerCase()).writeData(mWhat).endElement();

    
    }
}

