/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
package org.griphyn.cPlanner.classes;

import java.util.Iterator;
import java.util.List;


/**
 * This extends the job description used in Pegasus to incorporate the input and
 * the output url's that are required to pull in the input  and push out the
 * output files. In VDS terms each of these job is a VDS super node without the
 * registration job.
 *
 * @author Karan Vahi
 * @version $Revision: 1.3 $
 */

public class GRMSJob extends SubInfo {

    /**
     * List of the input urls from where to fetch the input files.
     * Each object in this list is of type <code>NameValue</code> where name is
     * the logical name of the associated file , and value it the url.
     */
    private List inputURLs;

    /**
     * List of the output urls that specify where to transfer the output files.
     * Each object in this list is of type <code>NameValue</code> where name is
     * the logical name of the associated file , and value it the url.
     */
    private List outputURLs;

    /**
     * The default constructor
     */
    public GRMSJob() {
        super();
        inputURLs = new java.util.ArrayList();
        outputURLs = new java.util.ArrayList();
    }


    /**
     * The overloaded construct that constructs a GRMS job by wrapping around
     * the SubInfo job.
     *
     * @param job  the original job description.
     */
    public GRMSJob(SubInfo job) {
        super(job);
        inputURLs = new java.util.ArrayList();
        outputURLs = new java.util.ArrayList();
   }


   /**
    * Adds a url of the job. The url can be either input or output.
    *
    * @param lfn  the lfn associated with the url.
    * @param url  the url.
    * @param type i means input
    *             o means output
    *
    * @return boolean true  the url was successfully added.
    *                 false the type of the url was wrong.
    */
   public boolean addURL(String lfn, String url, char type){
       boolean ret = false;
       NameValue nv;

       switch(type){
           case 'i':
               nv  = new NameValue(lfn,url);
               inputURLs.add(nv);
               ret = true;
               break;

           case 'o':
               nv  = new NameValue(lfn,url);
               outputURLs.add(nv);
               ret = true;
               break;

           default:
               break;

       }

       return ret;
   }

   /**
    * It returns an iterator to the underlying associated collection.
    * The iterator returns objects of type <code>NameValue</code>.
    *
    * @param type i input urls
    *             o output urls
    *
    * @return the iterator to corresponding collection.
    *         null if invalid type.
    */
   public Iterator iterator(char type){
       Iterator it;
       switch(type){
           case 'i':
               it = inputURLs.iterator();
               break;

           case 'o':
               it = outputURLs.iterator();
               break;

           default:
               it = null;
               break;

       }
       return it;

   }
   /**
     * Returns a textual description of the GRMS job.
     */
    public String toString(){
        String st = super.toString();
        Iterator it = inputURLs.iterator();

        st += "\nInput URLs";
        while(it.hasNext()){
            st += (String)it.next() + ",";
        }
        it = inputURLs.iterator();
        st += "\nOutput URLs";
        while(it.hasNext()){
            st += (String)it.next() + ",";
        }


        return st;

    }

    /**
     * Returns a new copy of the Object. However the url list are copied by
     * reference. It is not a true copy.
     */
    public Object clone(){
        GRMSJob newJob    = new GRMSJob((SubInfo)super.clone());
        newJob.inputURLs  = this.inputURLs;
        newJob.outputURLs = this.outputURLs;
        return newJob;

    }
}