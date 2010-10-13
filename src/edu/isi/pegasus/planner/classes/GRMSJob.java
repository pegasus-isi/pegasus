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

package edu.isi.pegasus.planner.classes;

import java.util.Iterator;
import java.util.List;


/**
 * This extends the job description used in Pegasus to incorporate the input and
 * the output url's that are required to pull in the input  and push out the
 * output files. In VDS terms each of these job is a VDS super node without the
 * registration job.
 *
 * @author Karan Vahi
 * @version $Revision$
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
