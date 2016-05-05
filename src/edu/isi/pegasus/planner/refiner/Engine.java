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


package edu.isi.pegasus.planner.refiner;


import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;

import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PlannerOptions;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.planner.common.PegasusProperties;


import edu.isi.pegasus.planner.catalog.TransformationCatalog;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.classes.Job;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.mapper.SubmitMapper;
import java.io.File;
import java.io.IOException;
import org.griphyn.vdl.euryale.FileFactory;

/**
 * The  class which is a superclass of all the various Engine classes. It
 * defines common methods and member variables.
 *
 * @author Karan Vahi
 * @author Gaurang Mehta
 * @version $Revision$
 *
 */
public abstract  class Engine {

    //constants
    public static final String REGISTRATION_UNIVERSE = "registration";
    public static final String TRANSFER_UNIVERSE = "transfer";

    /**
     * The pool on which all the output data should be transferred.
     */
    protected static String mOutputPool;

    /**
     * The object holding all the properties pertaining to Pegasus.
     */
    protected PegasusProperties mProps;

    
    /**
     * The handle to the Transformation Catalog. It must be instantiated
     * in the implementing class.
     */
    protected TransformationCatalog mTCHandle;

    
    /**
     * The handle to the Pool Info Provider. It is instantiated in this class
     */
    protected SiteStore mSiteStore;


    /**
     * Contains the message which is to be logged by Pegasus.
     */
    protected String mLogMsg = "";


    /**
     * Defines the read mode for transformation catalog. Whether we want to read all
     * at once or as desired.
     *
     * @see org.griphyn.common.catalog.transformation.TCMode
     */
    protected String mTCMode;

  


    /**
     * The logging object which is used to log all the messages.
     *
     */
    protected LogManager mLogger ;
    
    /**
     * Contains the various options to the Planner as passed by the user at
     * runtime.
     */
    protected PlannerOptions mPOptions;
    
    /**
     * The bag of initialization objects
     */
    protected PegasusBag mBag;

    /**
     * Handle to the Submit directory factory, that returns the relative
     * submit directory for a job
     */
    protected SubmitMapper mSubmitDirFactory;
    
    /**
     *
     *
     *
     * @param bag      bag of initialization objects
     */
    public Engine( PegasusBag bag ) {
        mBag      = bag;
        mLogger   = bag.getLogger();
        mProps    = bag.getPegasusProperties() ;
        mPOptions = bag.getPlannerOptions();
        mTCHandle = bag.getHandleToTransformationCatalog();
        mSiteStore= bag.getHandleToSiteStore();
        mSubmitDirFactory = bag.getSubmitDirFileFactory();
        loadProperties();
    }
    
    

    /**
     * Loads all the properties that are needed by the Engine classes.
     */
    public void loadProperties() {

    }




    /**
     * Returns true if a particular String is in the Vector of strings.
     *
     * @param stringName  the String which has to be searched for in the Vector.
     * @param vector      the Vector of Strings in which to search for a
     *                    particular String.
     *
     * @return    boolean on the basis of whether the String  in Vector or not.
     */
    public boolean stringInVector(String stringName, Vector vector) {
        Enumeration e = vector.elements();
        while (e.hasMoreElements()) {
            if (stringName.equalsIgnoreCase( (String) e.nextElement())) {
                return true;
            }
        }
        return false;
    }

    public boolean stringInList(String stringName, List list) {
        if (list.contains(stringName)) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Returns true if a particular String is in the Vector of PegasusFile objects.
     *
     * @param stringName the String which has to be searched for in the Vector.
     * @param vector     the Vector of Strings in which to search for a particular
     *                   String
     *
     * @return    boolean on the basis of whether the String in Vector or not.
     *
     */
    public boolean stringInPegVector(String stringName, Vector vector) {
        Enumeration e = vector.elements();
        while (e.hasMoreElements()) {
            PegasusFile pf = (PegasusFile) e.nextElement();
            if (stringName.equalsIgnoreCase(pf.getLFN())) {
                return true;
            }
        }
        return false;
    }

    /**
     * Adds elements (PegasusFile type) in a Vector to another Vector and
     * returns the new Vector.
     *
     * @param from_vector  the source
     * @param to_vector    the destination
     *
     * @return   Vector of PegasusFile objects
     */
    public Vector addVector(Vector from_vector, Vector to_vector) {
        Enumeration e = from_vector.elements();
        Vector newVector = (Vector) to_vector.clone();

        while (e.hasMoreElements()) {
            PegasusFile pf = (PegasusFile) e.nextElement();
            newVector.addElement(pf);
            /*String elem = new String((String)e.nextElement());
                         if(!stringInVector(elem,to_vector)){
                newVector.addElement(elem);
                         }*/
        }

        return newVector;
    }

    /**
     * It prints the contents of the Vector, with the first line being the heading.
     *
     * @param heading   The heading you want to give to the text which is printed.
     * @param vector    The <code>Vector</code> whose elements you want to print.
     */
    public void printVector(String heading, Vector vector) {
        mLogger.log(heading, LogManager.DEBUG_MESSAGE_LEVEL);
        for(Iterator it = vector.iterator() ; it.hasNext() ;) {
            mLogger.log( it.next().toString() , LogManager.DEBUG_MESSAGE_LEVEL);
        }
    }

    /**
     * It prints the contents of the Vector, to a String with the first line being
     * the heading.
     *
     * @param heading   The heading you want to give to the text which is printed.
     * @param vector    The <code>Vector</code> whose elements you want to print.
     *
     * @return String
     */
    public String vectorToString(String heading, Vector vector) {
        Enumeration e = vector.elements();
        String st = heading;
        while (e.hasMoreElements()) {
            st += "\t" + e.nextElement();
        }
        return st;
    }

    /**
     * It appends the source list at the end of the destination list.
     *
     * @param dest    the destination list
     * @param source  the source list
     */
    public void appendArrayList(ArrayList dest, ArrayList source) {

        Iterator iter = source.iterator();
        while (iter.hasNext()) {
            dest.add(iter.next());
        }
    }
    
   
    /**
     * Complains for head node url prefix not specified
     * 
     * @param refiner the name of the refiner
     * @param site   the site handle
     * 
     * @throws RuntimeException when URL Prefix cannot be determined for various reason.
     */
    protected void complainForHeadNodeURLPrefix( String refiner, String site, FileServer.OPERATION operation) {
         this.complainForHeadNodeURLPrefix( refiner,site, operation, null  );
    }

    /**
     * Complains for head node url prefix not specified
     * 
     * @param refiner the name of the refiner
     * @param operation  the operation for which error is throw
     * @param job    the related job if any
     * @param site   the site handle
     * 
     * @throws RuntimeException when URL Prefix cannot be determined for various reason.
     */
    protected void complainForHeadNodeURLPrefix(String refiner, String site, FileServer.OPERATION operation, Job job   ) {
        StringBuffer error = new StringBuffer();
        error.append( "[" ).append( refiner ).append( "] ");
        if( job != null ){
            error.append( "For job (" ).append( job.getID() ).append( ")." );
        }
        error.append( "Unable to determine URL Prefix for the FileServer ").
              append( " for operation ").append( operation ).append( " for shared scratch file system on site: " ).
              append( site );
        throw new RuntimeException( error.toString() );
    }
    
   
}
