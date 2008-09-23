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



package org.griphyn.cPlanner.classes;

import edu.isi.pegasus.common.logging.LogManager;

import org.globus.replica.rls.RLSClient;
import org.globus.replica.rls.RLSString2Bulk;


/**
 * A class that stores the results
 * of querying an LRC. It includes
 * whether the operation was a success
 * or not and in addition the value
 * of the pool attribute.
 *
 * @author Gaurang Mehta
 * @author Karan Vahi
 * @version $Revision$
 *
 * @see org.globus.replica.rls.RLSString2Bulk
 */

public class LRCResult extends Data {

    /**
     * The lfn for which the LRC
     * was queried.
     */
    public String lfn;

    /**
     * The pfn associated
     * with the lfn, if an
     * entry found in the LRC.
     * Else it can be null.
     */
    public String pfn;

    /**
     * The pool attribute associated
     * with the pfn returned.
     * This should be set to null
     * if pfn is not found.
     */
    public String pool;

    /**
     * The status of the operation.
     * Whether it was a sucess or not.
     * The status are defined in
     * RLSClient.java
     *
     * @see org.globus.replica.rls.RLSClient
     */
    public int LRCExitStatus;



    /**
     * The default constructor
     */
    public LRCResult() {
        lfn = new String();
        pfn = new String();
        pool= new String();
        LRCExitStatus = 0;
    }

    /**
     * The overloaded constructor.
     * Takes in RLSString2Bulk
     * object.
     */
    public LRCResult(RLSString2Bulk s2, String poolAtt){
        lfn = s2.s1;
        pfn = s2.s2;
        LRCExitStatus = s2.rc;
        pool = poolAtt;
    }

    /**
     * Returns a clone of the
     * existing object.
     */
    public Object clone(){
        LRCResult res = new LRCResult();

        res.lfn           = this.lfn;
        res.pfn           = this.pfn;
        res.pool          = this.pool;
        res.LRCExitStatus = this.LRCExitStatus;

        return res;
    }

    /**
     * Returns a textual description
     * of the object.
     */
    public String toString(){
        String str = "\n lfn: " + lfn +
                     " exit status: " + getErrorMessage()+
                     " pfn: " + pfn +
                     " pool: " + pool;
        return str;


    }

    /**
     * Returns the error/status
     * message according to
     * the LRCExitStatus.
     */
    public String getErrorMessage(){
        RLSClient rls = null;

        try{
            rls = new RLSClient();
        }
        catch(Exception e){
            mLogger.log("Exception while initialising to RLS" + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
        }
        String err = rls.getErrorMessage(this.LRCExitStatus);

        return err;
    }

}
