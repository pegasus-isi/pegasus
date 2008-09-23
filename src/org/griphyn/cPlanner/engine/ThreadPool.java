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

import edu.isi.pegasus.common.logging.LogManagerFactory;
import java.io.File;
import java.io.FileInputStream;
import java.util.LinkedList;
import java.util.Set;

import org.gridforum.jgss.ExtendedGSSCredential;
import org.gridforum.jgss.ExtendedGSSManager;
import org.griphyn.cPlanner.classes.AuthenticateRequest;
import org.griphyn.cPlanner.classes.Profile;
import edu.isi.pegasus.common.logging.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.namespace.ENV;
import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;
import org.griphyn.cPlanner.poolinfo.PoolMode;
import org.ietf.jgss.GSSCredential;


/**
 * This maintains a pool of authenticate threads that authenticate against a
 * particular resource.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class ThreadPool {

    /**
     * The maximum number of authentication threads that are spawned.
     */
    public static final int NUMBER_OF_THREADS = 5;

    /**
     * The request queue that holds the authenticate requests. The worker
     * threads do access this job queue.
     */
    private LinkedList mQueue;

    /**
     * The handle to the properties object.
     */
    private PegasusProperties mProps;

    /**
     * The handle to the Pool Info Provider.
     */
    private PoolInfoProvider mPoolHandle;

    /**
     * The handle to the LogManager object.
     */
    private LogManager mLogger;

    /**
     * The Set of pools that need to be authenticated against.
     */
    private Set mExecPools;

    /**
     * The number of pools that one has to authenticate against.
     */
    private Integer mNumOfPools;

    /**
     * The handle to the pool of threads that this thread pool is reponsible for.
     */
    private AuthenticateThread[] mWorkers;

    /**
     * The condition variable that is used to synchronize the shutdown.
     */
    private ConditionVariable mCurrentNum;

    /**
     * The namespace object holding the environment variables for local
     * pool.
     */
    private ENV mLocalEnv;

    /**
     * The credential loaded from the non default location if specified.
     */
    private GSSCredential mCredential;

    /**
     * The overloaded constructor.
     *
     * @param properties  the <code>PegasusProperties</code> to be used.
     * @param pools       the set of pools against which the user is authenticating.
     */
    public ThreadPool( PegasusProperties properties, Set pools ) {
        mQueue      = new LinkedList();
        mCurrentNum = new ConditionVariable();
        mProps      = properties;
        mLogger     =  LogManagerFactory.loadSingletonInstance( properties );
        String poolClass = PoolMode.getImplementingClass(mProps.getPoolMode());
        mPoolHandle = PoolMode.loadPoolInstance(poolClass,mProps.getPoolFile(),
                                                PoolMode.SINGLETON_LOAD);
        mExecPools  = pools;
        mNumOfPools = new Integer(pools.size());

        //load the local environment variables
        mLocalEnv   = loadLocalEnvVariables();
        //load the credential if the user has set the
        //corresponding environment variable.
        mCredential = (mLocalEnv.containsKey(ENV.X509_USER_PROXY_KEY))?
                        //load the proxy from the path specified
                        getGSSCredential((String)mLocalEnv.get(ENV.X509_USER_PROXY_KEY)):
                        null;

        if(mCredential == null){
            //log message
            mLogger.log("Proxy will be picked up from the default location in /tmp",
                        LogManager.DEBUG_MESSAGE_LEVEL);
        }

        //intialise the worker threads
        mWorkers = new AuthenticateThread[this.NUMBER_OF_THREADS];
        for(int i = 0; i < NUMBER_OF_THREADS; i++){
            mWorkers[i] = new AuthenticateThread(i);

            //start the threads
            mWorkers[i].start();
        }
    }


    /**
     * This method is called to ensure the clean shutdown of threads, and
     * waits till all the requests have been serviced.
     */
    public void shutdown(){

        //mNumOfPools is the CV on which you do a shutdowm
        synchronized(mCurrentNum){

            int numOfPools = mNumOfPools.intValue();
            for (int i = 0; i < NUMBER_OF_THREADS; i++) {
                //send the shutdown signal to the worker threads
                mWorkers[i].shutdown();
            }

            //wake up all the threads on this
            synchronized(mQueue){
                //mLogger.logMessage("Manager sending notify to all");
                mQueue.notifyAll();
            }

            while(mCurrentNum.getValue() < NUMBER_OF_THREADS){
                try{
                    mCurrentNum.wait();
                }
                catch(InterruptedException e){
                    mLogger.log(
                        "Manager got interrupted during shutdown" + e.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL);
                }
            }
        }

    }


    /**
     * Accepts an authentication request, that has to be serviced. It is added
     * to queue of requests.
     */
    public void acceptRequest(Object request){

        //see if any of the worker threads are available
        /*for(int i = 0; i < NUMBER_OF_THREADS; i++){
            if(mWorkers[i].isAvailable()){
                //no need to add to queue.
            }
        }*/

        synchronized(mQueue){
            mQueue.addLast(request);
            //send a notification to a worker thread
            mQueue.notify();
        }

    }


    /**
     * Reads in the environment variables into memory from the properties file
     * and the pool catalog.
     *
     * @return  the <code>ENV</code> namespace object holding the environment
     *          variables.
     */
    private ENV loadLocalEnvVariables(){
        //assumes that pool handle, and property handle are initialized.
        ENV env = new ENV();

        //load from the pool.config
        env.checkKeyInNS(mPoolHandle.getPoolProfile("local",Profile.ENV));
        //load from property file
        env.checkKeyInNS(mProps.getLocalPoolEnvVar());

        return env;
    }

    /**
     * Loads a GSSCredential from the proxy file residing at the path specified.
     *
     * @param file the path to the proxy file.
     *
     * @return GSSCredential
     *         null in case the file format is wrong, or file does not exist.
     */
    private GSSCredential getGSSCredential(String file){
        File f = new File(file);
        GSSCredential gcred = null;
        //sanity check first
        if(!f.exists()){
            return null;
        }

        try{
            byte[] data = new byte[ (int) f.length()];
            FileInputStream in = new FileInputStream(f);
            in.read(data);
            in.close();

            ExtendedGSSManager manager =
                (ExtendedGSSManager) ExtendedGSSManager.getInstance();

            gcred = manager.createCredential(data,
                                             ExtendedGSSCredential.IMPEXP_OPAQUE,
                                             GSSCredential.DEFAULT_LIFETIME,
                                             null,
                                             GSSCredential.INITIATE_AND_ACCEPT);
            mLogger.log("Loaded the credential from proxy file " + file,
                        LogManager.DEBUG_MESSAGE_LEVEL);

        }
        catch(Exception e){
            mLogger.log(
                "Unable to load proxy from file" + file  + " "  +
                e.getMessage(),LogManager.ERROR_MESSAGE_LEVEL);
        }
        return gcred;
    }

    /**
     * A thread as an inner class, that authenticates against one particular
     * pool.
     */
    class AuthenticateThread implements Runnable{

        /**
         * The pool against which to authenticate.
         */
        private String mPool;

        /**
         * The thread object that is used to launch the thread.
         */
        private Thread mThread;

        /**
         * Whether the thread is available to do some work or not.
         */
        private boolean mAvailable;

        /**
         * Whether to shutdown or not.
         */
        private boolean mShutdown;

        /**
         * The unique identifying id of the thread.
         */
        private int mIndex;

        /**
         * The overloaded constructor.
         *
         *
         */
        public AuthenticateThread(int index){
            mAvailable = true;
            mShutdown = false;
            mIndex = index;
        }

        /**
         * The start method for the thread. It initialises the thread and calls
         * it's start method.
         */
        public void start(){
            mThread = new Thread(this);
            mThread.start();
        }


        /**
         * Returns whether a thread is available to do some work or not.
         */
        public boolean isAvailable(){
            return mAvailable;
        }

        /**
         * Sets the shutdown flag to true. This does not make the thread stop.
         * The thread only stops when it's current request is serviced and the
         * queue is empty.
         */
        public void shutdown(){
            mShutdown = true;
        }

        /**
         * Calls the corresponding join method of the thread associated with
         * this class.
         *
         * @param millis   The time to wait in milliseconds.
         */
        public void join(long millis) throws InterruptedException{
            mThread.join(millis);
        }

        /**
         * The runnable method of the thread, that is called when the thread is
         * started.
         */
        public void run(){
            AuthenticateRequest ar;
            Authenticate a = new Authenticate( mProps, mPoolHandle );
            a.setCredential(mCredential);
            boolean authenticated = false;

            for(;;){
                //remain in an infinite loop and wait for a request to be released
                //from the queue.
                ar = getAuthenticateRequest();
                if(ar == null){
                    //no more requests to service and the shutdown signal has
                    //been received. send the notification to the manager and exit
                    mLogger.log("Thread [" + mIndex +"] got shutdown signal",
                                LogManager.DEBUG_MESSAGE_LEVEL);
                    synchronized(mCurrentNum){
                        mCurrentNum.increment();
                        mCurrentNum.notify();
                    }

                    break;
                }

                //means worker is busy, processing a request.
                mAvailable = false;
                //do the processing.
                authenticated = a.authenticate(ar);
                mLogger.log("Thread [" + mIndex +"] Authentication of " + ar +
                            " successful:" + authenticated,
                            LogManager.DEBUG_MESSAGE_LEVEL);
                if(!authenticated){
                    //we need to remove
                    boolean removal = a.removeResource(ar);
                    mLogger.log("Thread [" + mIndex +"] Removal of resource" + ar +
                                " successful:" + removal,LogManager.DEBUG_MESSAGE_LEVEL);
                }
                mAvailable = true;
                //be nice and sleep
                try{
                    mThread.sleep(5);
                }
                catch (InterruptedException ex) {
                    mLogger.log(
                        "Authenticate Thread [" + mIndex +"] got interrupted while waiting",
                        LogManager.DEBUG_MESSAGE_LEVEL);
                    //go into sleep again
                    continue;
                }

            }

        }

        /**
         * Returns an authentication request to the worker thread.
         *
         * @return  the authentication request.
         */
        public AuthenticateRequest getAuthenticateRequest(){
            synchronized(mQueue){

                for(;;){
                    if(mQueue.isEmpty() && mShutdown){
                        //no more requests to service and the shutdown signal has
                        //been received.
                        return null;
                    }
                    else if (mQueue.isEmpty()) {
                        //there is nothing in the queue so wait on it.
                        try {
                            mLogger.log("Thread [" + mIndex +"] going to wait",
                                        LogManager.DEBUG_MESSAGE_LEVEL);
                            mQueue.wait();
                            //again check for empty queue and shutdown signal
                            if(mQueue.isEmpty() && !mShutdown)
                                //go back to the wait state to receive a new
                                //request or a AR request
                                continue;
                        }
                        catch (InterruptedException ex) {
                            mLogger.log(
                                "Authenticate Thread [" + mIndex +"] got interrupted while waiting " +
                                ex.getMessage(),LogManager.ERROR_MESSAGE_LEVEL);
                            //go into sleep again
                            continue;
                        }

                    }
                    return (mQueue.isEmpty() && mShutdown)?
                           //indicates shutdown
                           null:
                           (AuthenticateRequest)mQueue.removeFirst();


                }

            }
        }

    }


    /**
     * A wrapper around an int that acts as a Condition Variable, and is used
     * as such. In behaviour it is probably closer to a semaphore.
     */
    class ConditionVariable{

        /**
         * The int that is associated with this object.
         */
        private int value;

        /**
         * The default constructor.
         */
        public ConditionVariable(){
            value = 0;
        }

        /**
         * It increments the value by 1.
         */
        public void increment(){
            value++;
        }

        /**
         * Returns the value.
         */
        public int getValue(){
            return value;
        }
    }

}
