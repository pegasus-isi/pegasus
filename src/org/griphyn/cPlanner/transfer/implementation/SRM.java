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


package org.griphyn.cPlanner.transfer.implementation;

import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.PlannerOptions;
import org.griphyn.cPlanner.classes.NameValue;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.namespace.VDS;

import org.griphyn.common.util.Separator;

import org.griphyn.common.catalog.TransformationCatalogEntry;

import org.griphyn.common.classes.TCType;

import java.util.List;
import java.util.Collection;
import java.util.Iterator;

import java.io.File;
import java.io.FileWriter;
import java.util.ArrayList;

/**
 * The implementation that is used to create transfer jobs that callout to
 * the SRMCP client, that can transfer files to and from a SRM server.
 *
 * <p>
 * In order to use the transfer implementation implemented by this class,
 * <pre>
 *        - the property pegasus.transfer.*.impl must be set to value SRM.
 * </pre>
 *
 * <p>
 * There should be an entry in the transformation catalog with the fully qualified
 * name as <code>SRM::srmcp</code> for all the sites where workflow is run,
 * or on the local site in case of third party transfers.
 *
 * <p>
 * The arguments with which the client is invoked can be specified
 * <pre>
 *       - by specifying the property pegasus.transfer.arguments
 *       - associating the Pegasus profile key transfer.arguments
 * </pre>
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class SRM extends AbstractMultipleFTPerXFERJob {

    /**
     * The transformation namespace for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "srm";


    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "srmcp";

    /**
     * The version number for the transfer job.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for for the transfer job.
     */
    public static final String DERIVATION_NAMESPACE = "srm";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "srmcp";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = "1.20";

    /**
     * A short description of the transfer implementation.
     */
    public static final String DESCRIPTION =
                    "SRM Client that talks to SRM server and does only one " +
                    "transfer per invocation";

    /**
     * The old file URL scheme that needs to be replaced.
     */
    private static final String OLD_FILE_URL_SCHEME = "file:///";

    /**
     * The new file URL scheme that replaces the old one.
     */
    private static final String NEW_FILE_URL_SCHEME = "file:////";


    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param properties  the properties object.
     * @param options     the options passed to the Planner.
     */
    public SRM(PegasusProperties properties, PlannerOptions options) {
        super(properties, options);
    }

    /**
     * Writes to a FileWriter stream the input for the SRM job.
     *
     * @param writer    the writer to the stdin file.
     * @param files    Collection of <code>FileTransfer</code> objects containing
     *                 the information about sourceam fin and destURL's.
     *
     *
     * @throws Exception
     */
    protected void writeJumboStdIn(FileWriter writer, Collection files) throws
        Exception {
        for(Iterator it = files.iterator();it.hasNext();){
            FileTransfer ft = (FileTransfer) it.next();
            NameValue source = ft.getSourceURL();
            NameValue dest   = ft.getDestURL();
            writer.write( sanitizeURL( source.getValue() ) );
            writer.write( " " );
            writer.write( sanitizeURL( dest.getValue() ) );
            writer.write( "\n" );
            writer.flush();
        }


    }

    /**
     * Constructs the arguments to the transfer executable that need to be
     * passed to the executable referred to in this transfer mode. The STDIN
     * of the job is passed as an argument.
     *
     * @param job   the object containing the transfer node.
     *
     * @return  the argument string
     */
    protected String generateArgumentString( TransferJob job ){
        StringBuffer sb = new StringBuffer();

        if( job.vdsNS.containsKey( VDS.TRANSFER_ARGUMENTS_KEY ) ){
            sb.append(
                      job.vdsNS.removeKey( VDS.TRANSFER_ARGUMENTS_KEY )
                      );
        }


        sb.append(" -copyjobfile ").append( job.getStdIn() );


        return sb.toString();
    }

    /**
     * Makes sure the stdin is transferred by the Condor File Transfer
     * Mechanism. In addition, the stdin is set to null, after the file has
     * been marked for transfer by Condor File Transfer Mechanism.
     *
     * @param job  the <code>TransferJob</code> that has been created.
     */
    public void postProcess( TransferJob job ){
        File f = new File( mPOptions.getSubmitDirectory(), job.getStdIn() );
        //add condor key transfer_input_files to transfer the file
        job.condorVariables.addIPFileForTransfer( f.getAbsolutePath() );
        job.setStdIn( "" );
    }


    /**
     * Returns a boolean indicating whether the transfer protocol being used by
     * the implementation preserves the X Bit or not while staging.
     *
     * @return boolean
     */
    public boolean doesPreserveXBit(){
        return false;
    }

    /**
     * Return a boolean indicating whether the transfers to be done always in
     * a third party transfer mode. A value of false, results in the
     * direct or peer to peer transfers being done.
     * <p>
     * A value of false does not preclude third party transfers. They still can
     * be done, by setting the property "pegasus.transfer.*.thirdparty.sites".
     *
     * @return boolean indicating whether to always use third party transfers
     *         or not.
     *
     * @see PegasusProperties#getThirdPartySites(String)
     */
    public boolean useThirdPartyTransferAlways(){
        return false;
    }

    /**
     * Returns a textual description of the transfer implementation.
     *
     * @return a short textual description
     */
    public  String getDescription(){
        return this.DESCRIPTION;
    }

    /**
     * Retrieves the transformation catalog entry for the executable that is
     * being used to transfer the files in the implementation.
     *
     * @param siteHandle  the handle of the  site where the transformation is
     *                    to be searched.
     *
     * @return  the transformation catalog entry if found, else null.
     */
    public TransformationCatalogEntry getTransformationCatalogEntry(String siteHandle){
        List tcentries = null;
        try {
            //namespace and version are null for time being
            tcentries = mTCHandle.getTCEntries(this.TRANSFORMATION_NAMESPACE,
                                               this.TRANSFORMATION_NAME,
                                               this.TRANSFORMATION_VERSION,
                                               siteHandle,
                                               TCType.INSTALLED);
        } catch (Exception e) {
            mLogger.log(
                "Unable to retrieve entry from TC for " + getCompleteTCName()
                + " :" + e.getMessage(),LogManager.ERROR_MESSAGE_LEVEL);
        }

        //see if any record is returned or not
        return(tcentries == null)?
               null:
              (TransformationCatalogEntry) tcentries.get(0);
    }


    /**
     * Returns the environment profiles that are required for the default
     * entry to sensibly work. Returns an empty list.
     *
     * @param site the site where the job is going to run.
     *
     * @return List of environment variables, else null in case where the
     *         required environment variables could not be found.
     */
    protected List getEnvironmentVariables( String site ){
        return new ArrayList();
    }


    /**
     * Returns the complete name for the transformation.
     *
     * @return the complete name.
     */
    protected String getCompleteTCName(){
        return Separator.combine(this.TRANSFORMATION_NAMESPACE,
                                 this.TRANSFORMATION_NAME,
                                 this.TRANSFORMATION_VERSION);
    }

    /**
     * Returns the namespace of the derivation that this implementation
     * refers to.
     *
     * @return the namespace of the derivation.
     */
    protected String getDerivationNamespace(){
        return this.DERIVATION_NAMESPACE;
    }


    /**
     * Returns the logical name of the derivation that this implementation
     * refers to.
     *
     * @return the name of the derivation.
     */
    protected String getDerivationName(){
        return this.DERIVATION_NAME;
    }

    /**
     * Returns the version of the derivation that this implementation
     * refers to.
     *
     * @return the version of the derivation.
     */
    protected String getDerivationVersion(){
        return this.DERIVATION_VERSION;
    }

    /**
     * Method that sanitizes file URL's to match with the SRM client.
     * It replaces file:/// with file://// in the URL.
     *
     * @param url  the URL to be santized.
     *
     * @return the sanitized URL if it is a file URL, else the URL that is
     *         passed.
     */
    private String sanitizeURL(String url){
        if(url.startsWith(this.OLD_FILE_URL_SCHEME)){
            //check if there is already a /
            if(url.length() > 8){
                char c = url.charAt(8);
                if(c != '/'){//should actually be File.separator
                    //insert the 4th /
                    return this.NEW_FILE_URL_SCHEME + url.substring(8);
                }
            }
        }
        return url;
    }
}
