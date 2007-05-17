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
package org.griphyn.cPlanner.common;
import java.util.ArrayList;
import java.util.Iterator;

import org.globus.replica.rls.RLSClient;
import org.globus.replica.rls.RLSException;
import org.globus.replica.rls.RLSLRCInfo;
import org.globus.replica.rls.RLSRLIInfo;
import org.globus.replica.rls.RLSStats;

/**
 * This class provides the statistics of the RLS server
 * by connecting to the server from the RLS client.
 *
 * @author Sonal Patil
 * @version $Revision: 1.4 $
 */


public class StatRLS {

    RLSClient rlsclient;

    /**
     * The constructor
     * @param url The URL of the RLS to query for statistics
     */
    public StatRLS (String url) {
	try {
	    rlsclient = new RLSClient(url);
	}
	catch (RLSException e) {
	    System.out.println("Exception in client: "+ e);
	}

    }

    public static void main (String [] argv) {

    	StatRLS stat_rls = new StatRLS(argv[0]);
	try {
	    RLSStats rlsstats = stat_rls.rlsclient.Stats();
	    System.out.println("Version:     "+rlsstats.Version);
	    System.out.println("uptime:      "+(rlsstats.Uptime/3600)+":"+((rlsstats.Uptime/60)%60)+":"+(rlsstats.Uptime%60));
	    if((rlsstats.Flags & rlsstats.RLS_LRCSERVER) != 0)
		System.out.println("LRC stats");
	    if ((rlsstats.Flags & rlsstats.RLS_SNDLFNLIST) != 0)
		System.out.println("  update method: lfnlist");
	    if ((rlsstats.Flags & rlsstats.RLS_SNDBLOOMFILTER) != 0)
		System.out.println("  update method: bloomfilter");
	    try {
		RLSClient.LRC lrc = stat_rls.rlsclient.getLRC();
		ArrayList lrc_rli_list = lrc.rliList();
		//ArrayList lrc_rli_list = stat_rls.rlsclient.LRCRLIList();
		Iterator itr = lrc_rli_list.iterator();
		while (itr.hasNext()) {
		    RLSRLIInfo rls_rli_info = ( RLSRLIInfo) itr.next();
		    if(rls_rli_info.flags == 1)
			System.out.println("  updates bloomfilter: "+rls_rli_info.url+"  last "+rls_rli_info.lastupdate);
		    else
			System.out.println("  updates lfnlist: "+rls_rli_info.url+"  last "+rls_rli_info.lastupdate);
		    //System.out.println("update interval:"+rls_rli_info.updateinterval);

		}
	    }

	    catch (RLSException e) {
	    }
	    System.out.println("  lfnlist update interval: "+rlsstats.LRCLFNListUI);
	    System.out.println("  bloomfilter update interval: "+rlsstats.LRCBloomFilterUI);

	    System.out.println("  numlfn: "+ rlsstats.LRCNumLFN);
	    System.out.println("  numpfn: "+ rlsstats.LRCNumPFN);
	    System.out.println("  nummap: "+ rlsstats.LRCNumMAP);




	if((rlsstats.Flags & rlsstats.RLS_RLISERVER) != 0)
	    System.out.println("RLI stats");
	 try {
	     RLSClient.RLI rli = stat_rls.rlsclient.getRLI();
	     ArrayList rli_lrc_list = rli.lrcList();
	     //ArrayList rli_lrc_list = stat_rls.rlsclient.RLILRCList();
		Iterator itr = rli_lrc_list.iterator();
		while (itr.hasNext()) {
		    RLSLRCInfo rls_lrc_info = ( RLSLRCInfo) itr.next();
		    System.out.println("  updated by: "+rls_lrc_info.url+"  last  "+rls_lrc_info.lastupdate);
		}
	 }
	 catch (RLSException e) {
	 }


	 if ((rlsstats.Flags & rlsstats.RLS_RCVBLOOMFILTER) !=0 )
	     System.out.println("  updated via bloomfilters");
	 else if ((rlsstats.Flags & rlsstats.RLS_RCVLFNLIST) !=0) {
	    System.out.println("  updated via lfnlists");
	    System.out.println("  numlfn: "+ rlsstats.RLINumLFN);
	    System.out.println("  numlrc: "+ rlsstats.RLINumLRC);
	    System.out.println("  nummap: "+ rlsstats.RLINumMAP);
	  }


	}
    	catch (RLSException e) {
	}

    }
}
