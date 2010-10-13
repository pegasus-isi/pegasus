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

package org.griphyn.cPlanner.engine;

import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.classes.AuthenticateRequest;
import edu.isi.pegasus.planner.catalog.site.impl.old.classes.GridFTPServer;
import edu.isi.pegasus.planner.catalog.site.impl.old.classes.JobManager;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.common.PegasusProperties;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import edu.isi.pegasus.planner.classes.PegasusBag;

/**
 * It authenticates the user with the sites, that the user specifies at the
 * execution time. It spawns out a thread for each pool that authenticates
 * against the jobmanager for the vanilla universe as specified in the pool
 * configuration file.
 *
 * @author Karan Vahi
 * @version $Revision$
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
/*    public AuthenticateEngine( PegasusProperties props, Set pools) {
        super( props );
        mExecPools = pools;

    }
*/ 

    /**
     * The overloaded constructor.
     *
     * @param bag    the <code>PegasusBag</code> to be used.
     * @param pools  The set of pools against which you want to authenticate the
     *               user.
     */
    public AuthenticateEngine( PegasusBag bag, Set pools) {
        super( bag );
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
        GridGateway jm;
        FileServer gserv;
        String contact;

        //we need synchronization to ensure that an threads are started only
        //when all the requests have been sent to the threadpool, as this
        //failure to authenticate against a pool leads to it's removal from
        //this set.
        synchronized(mExecPools){
            while(it.hasNext()){
                pool = (String)it.next();

 //               List jmList =  mPoolHandle.getJobmanagers(pool);
 //               Iterator it1 = jmList.iterator();
                for( Iterator it1 = mSiteStore.lookup( pool ).getGridGatewayIterator(); it1.hasNext() ;){
                    jm = (GridGateway)it1.next();
//                    contact = jm.getInfo(JobManager.URL);
                    AuthenticateRequest ar = new AuthenticateRequest('j',pool, jm.getContact());
                    manager.acceptRequest(ar);
                }

//                List gridFtpList = mPoolHandle.getGridFTPServers(pool);
//                it1 = gridFtpList.iterator();
//                while(it1.hasNext()){
                for( Iterator it1 = mSiteStore.lookup( pool ).getFileServerIterator(); it1.hasNext();){
                    gserv = ( FileServer )it1.next();
//                    contact = gserv.getInfo(GridFTPServer.GRIDFTP_URL);
                    AuthenticateRequest ar = new AuthenticateRequest('g',pool, gserv.getURLPrefix() );
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
            l = mSiteStore.lookup( pool ).getFileServers();
            if(l == null || l.isEmpty()){
                mLogger.log("Removing Exec pool " + pool +
                            "  as no authenticated gridftp server",
                            LogManager.DEBUG_MESSAGE_LEVEL);
                it.remove();
                continue;
            }

            List l1 = mSiteStore.lookup( pool ).getGridGateways( );
//            List l1 = mPoolHandle.getJobmanagers(pool,"transfer");
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
/*
        AuthenticateEngine a = new AuthenticateEngine( PegasusProperties.getInstance(),s);
        a.mLogger.setLevel(1);

        a.authenticate();

        System.out.println("Authentication Done!!");
        System.out.println(a.mPoolHandle.getGridFTPServers("isi_lsf"));
        a.mLogger.log("Vanilla JMS " + a.mPoolHandle.getJobmanagers("isi_lsf"),
                      LogManager.DEBUG_MESSAGE_LEVEL);
*/ 

    }


}
