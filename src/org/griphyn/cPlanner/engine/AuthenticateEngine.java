/**
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found at $PEGASUS_HOME/GTPL or
 * http://www.globus.org/toolkit/download/license.html.
 * This notice must appear in redistributions of this file
 * with or without modification.
 *
 * Redistributions of this Software, with or without modification, must reproduce
 * the GTPL in:
 * (1) the Software, or
 * (2) the Documentation or
 * some other similar material which is provided with the Software (if any).
 *
 * Copyright 1999-2004
 * University of Chicago and The University of Southern California.
 * All rights reserved.
 */
package org.griphyn.cPlanner.engine;

import org.griphyn.cPlanner.classes.AuthenticateRequest;
import org.griphyn.cPlanner.classes.GridFTPServer;
import org.griphyn.cPlanner.classes.JobManager;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * It authenticates the user with the sites, that the user specifies at the
 * execution time. It spawns out a thread for each pool that authenticates
 * against the jobmanager for the vanilla universe as specified in the pool
 * configuration file.
 *
 * @author Karan Vahi
 * @version $Revision: 1.5 $
 */
public class AuthenticateEngine extends Engine {

    /**
     * The Set of pools that need to be authenticated against.
     */
    private Set mExecPools;


    /**
     * The overloaded constructor.
     *
     * @param props  the <code>PegasusProperties</code> to be used.
     * @param pools  The set of pools against which you want to authenticate the
     *               user.
     */
    public AuthenticateEngine( PegasusProperties props, Set pools) {
        super( props );
        mExecPools = pools;

    }

    /**
     * It returns a set of pools against which the user can authenticate to.
     *
     * @return  the set of authenticated pools.
     */
    public Set authenticate(){
        Iterator it = mExecPools.iterator();
        ThreadPool manager = new ThreadPool( mProps, mExecPools);
        String pool;
        JobManager jm;
        GridFTPServer gserv;
        String contact;

        //we need synchronization to ensure that an threads are started only
        //when all the requests have been sent to the threadpool, as this
        //failure to authenticate against a pool leads to it's removal from
        //this set.
        synchronized(mExecPools){
            while(it.hasNext()){
                pool = (String)it.next();

                List jmList = mPoolHandle.getJobmanagers(pool);
                Iterator it1 = jmList.iterator();
                while(it1.hasNext()){
                    jm = (JobManager)it1.next();
                    contact = jm.getInfo(JobManager.URL);
                    AuthenticateRequest ar = new AuthenticateRequest('j',pool,contact);
                    manager.acceptRequest(ar);
                }

                List gridFtpList = mPoolHandle.getGridFTPServers(pool);
                it1 = gridFtpList.iterator();
                while(it1.hasNext()){
                    gserv = (GridFTPServer)it1.next();
                    contact = gserv.getInfo(GridFTPServer.GRIDFTP_URL);
                    AuthenticateRequest ar = new AuthenticateRequest('g',pool,contact);
                    manager.acceptRequest(ar);

                }
            }
        }
        manager.shutdown();
        purgePools();

        return mExecPools;
    }


    /**
     * It removies from the list of pools the pool that was not authenticated
     * against. It queries the soft state of the pool config to see if there
     * are at least one jobmanager and gridftp server on the pool.
     * Due to the authentication the unauthenticated jobmanagers and servers
     * would have been removed from the soft state of the pool config.
     */
    private synchronized void purgePools(){
        Iterator it = mExecPools.iterator();
        String pool;
        List l;

        while(it.hasNext()){
            pool = (String)it.next();
            l = mPoolHandle.getGridFTPServers(pool);
            if(l == null || l.isEmpty()){
                mLogger.log("Removing Exec pool " + pool +
                            "  as no authenticated gridftp server",
                            LogManager.DEBUG_MESSAGE_LEVEL);
                it.remove();
                continue;
            }

            l = mPoolHandle.getJobmanagers(pool,"vanilla");
            List l1 = mPoolHandle.getJobmanagers(pool,"transfer");
            if( (l == null || l.isEmpty()) ||
                (l1 == null || l1.isEmpty())){
                //we have no jobmanagers for universe vanilla or transfer universe
                mLogger.log("Removing Exec pool " + pool +
                            " as no authenticated jobmanager",
                            LogManager.DEBUG_MESSAGE_LEVEL);
                it.remove();
                continue;
            }

        }

    }




    /**
     * The main testing method.
     *
     */
    public static void main(String[] args){
        Set s = new HashSet();
        //s.add("isi_condor");
        s.add("isi_lsf");

        AuthenticateEngine a = new AuthenticateEngine( PegasusProperties.getInstance(),s);
        a.mLogger.setLevel(1);

        a.authenticate();

        System.out.println("Authentication Done!!");
        System.out.println(a.mPoolHandle.getGridFTPServers("isi_lsf"));
        a.mLogger.log("Vanilla JMS " + a.mPoolHandle.getJobmanagers("isi_lsf"),
                      LogManager.DEBUG_MESSAGE_LEVEL);

    }


}
