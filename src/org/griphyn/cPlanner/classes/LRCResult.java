/*
 * Globus Toolkit Public License (GTPL)
 *
 * Copyright (c) 1999 University of Chicago and The University of
 * Southern California. All Rights Reserved.
 *
 *  1) The "Software", below, refers to the Globus Toolkit (in either
 *     source-code, or binary form and accompanying documentation) and a
 *     "work based on the Software" means a work based on either the
 *     Software, on part of the Software, or on any derivative work of
 *     the Software under copyright law: that is, a work containing all
 *     or a portion of the Software either verbatim or with
 *     modifications.  Each licensee is addressed as "you" or "Licensee."
 *
 *  2) The University of Southern California and the University of
 *     Chicago as Operator of Argonne National Laboratory are copyright
 *     holders in the Software.  The copyright holders and their third
 *     party licensors hereby grant Licensee a royalty-free nonexclusive
 *     license, subject to the limitations stated herein and
 *     U.S. Government license rights.
 *
 *  3) A copy or copies of the Software may be given to others, if you
 *     meet the following conditions:
 *
 *     a) Copies in source code must include the copyright notice and
 *        this license.
 *
 *     b) Copies in binary form must include the copyright notice and
 *        this license in the documentation and/or other materials
 *        provided with the copy.
 *
 *  4) All advertising materials, journal articles and documentation
 *     mentioning features derived from or use of the Software must
 *     display the following acknowledgement:
 *
 *     "This product includes software developed by and/or derived from
 *     the Globus project (http://www.globus.org/)."
 *
 *     In the event that the product being advertised includes an intact
 *     Globus distribution (with copyright and license included) then
 *     this clause is waived.
 *
 *  5) You are encouraged to package modifications to the Software
 *     separately, as patches to the Software.
 *
 *  6) You may make modifications to the Software, however, if you
 *     modify a copy or copies of the Software or any portion of it,
 *     thus forming a work based on the Software, and give a copy or
 *     copies of such work to others, either in source code or binary
 *     form, you must meet the following conditions:
 *
 *     a) The Software must carry prominent notices stating that you
 *        changed specified portions of the Software.
 *
 *     b) The Software must display the following acknowledgement:
 *
 *        "This product includes software developed by and/or derived
 *         from the Globus Project (http://www.globus.org/) to which the
 *         U.S. Government retains certain rights."
 *
 *  7) You may incorporate the Software or a modified version of the
 *     Software into a commercial product, if you meet the following
 *     conditions:
 *
 *     a) The commercial product or accompanying documentation must
 *        display the following acknowledgment:
 *
 *        "This product includes software developed by and/or derived
 *         from the Globus Project (http://www.globus.org/) to which the
 *         U.S. Government retains a paid-up, nonexclusive, irrevocable
 *         worldwide license to reproduce, prepare derivative works, and
 *         perform publicly and display publicly."
 *
 *     b) The user of the commercial product must be given the following
 *        notice:
 *
 *        "[Commercial product] was prepared, in part, as an account of
 *         work sponsored by an agency of the United States Government.
 *         Neither the United States, nor the University of Chicago, nor
 *         University of Southern California, nor any contributors to
 *         the Globus Project or Globus Toolkit nor any of their employees,
 *         makes any warranty express or implied, or assumes any legal
 *         liability or responsibility for the accuracy, completeness, or
 *         usefulness of any information, apparatus, product, or process
 *         disclosed, or represents that its use would not infringe
 *         privately owned rights.
 *
 *         IN NO EVENT WILL THE UNITED STATES, THE UNIVERSITY OF CHICAGO
 *         OR THE UNIVERSITY OF SOUTHERN CALIFORNIA OR ANY CONTRIBUTORS
 *         TO THE GLOBUS PROJECT OR GLOBUS TOOLKIT BE LIABLE FOR ANY
 *         DAMAGES, INCLUDING DIRECT, INCIDENTAL, SPECIAL, OR CONSEQUENTIAL
 *         DAMAGES RESULTING FROM EXERCISE OF THIS LICENSE AGREEMENT OR
 *         THE USE OF THE [COMMERCIAL PRODUCT]."
 *
 *  8) LICENSEE AGREES THAT THE EXPORT OF GOODS AND/OR TECHNICAL DATA
 *     FROM THE UNITED STATES MAY REQUIRE SOME FORM OF EXPORT CONTROL
 *     LICENSE FROM THE U.S. GOVERNMENT AND THAT FAILURE TO OBTAIN SUCH
 *     EXPORT CONTROL LICENSE MAY RESULT IN CRIMINAL LIABILITY UNDER U.S.
 *     LAWS.
 *
 *  9) Portions of the Software resulted from work developed under a
 *     U.S. Government contract and are subject to the following license:
 *     the Government is granted for itself and others acting on its
 *     behalf a paid-up, nonexclusive, irrevocable worldwide license in
 *     this computer software to reproduce, prepare derivative works, and
 *     perform publicly and display publicly.
 *
 * 10) The Software was prepared, in part, as an account of work
 *     sponsored by an agency of the United States Government.  Neither
 *     the United States, nor the University of Chicago, nor The
 *     University of Southern California, nor any contributors to the
 *     Globus Project or Globus Toolkit, nor any of their employees,
 *     makes any warranty express or implied, or assumes any legal
 *     liability or responsibility for the accuracy, completeness, or
 *     usefulness of any information, apparatus, product, or process
 *     disclosed, or represents that its use would not infringe privately
 *     owned rights.
 *
 * 11) IN NO EVENT WILL THE UNITED STATES, THE UNIVERSITY OF CHICAGO OR
 *     THE UNIVERSITY OF SOUTHERN CALIFORNIA OR ANY CONTRIBUTORS TO THE
 *     GLOBUS PROJECT OR GLOBUS TOOLKIT BE LIABLE FOR ANY DAMAGES,
 *     INCLUDING DIRECT, INCIDENTAL, SPECIAL, OR CONSEQUENTIAL DAMAGES
 *     RESULTING FROM EXERCISE OF THIS LICENSE AGREEMENT OR THE USE OF
 *     THE SOFTWARE.
 *
 *                               END OF LICENSE
 */


package org.griphyn.cPlanner.classes;

import org.griphyn.cPlanner.common.LogManager;

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
 * @version $Revision: 1.3 $
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