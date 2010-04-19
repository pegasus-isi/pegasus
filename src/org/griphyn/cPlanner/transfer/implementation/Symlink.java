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

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.FileTransfer;
import org.griphyn.cPlanner.classes.PegasusBag;

import org.griphyn.cPlanner.namespace.VDS;

import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;

import java.io.FileWriter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import org.griphyn.common.util.Separator;

/**
 * The implementation that creates transfer jobs referring to the symlink
 * executable distributed with Pegasus.
 *
 * The input format for the symlink executable is the same as the transfer
 * executable.
 *
 * The symlink executable differs in the following ways
 *      - It only symlinks files. Hence both source and destination URL's
 *        should be file URL's
 *      - is not dependent on Globus. Hence does not require globus client
 *        libraries installed.
 *      - should always be executed in the two party mode.
 * <p>
 *
 * Use of this executable only makes sense with symlink jobs.
 *
 * In order to use the transfer implementation implemented by this class, the
 * property <code>pegasus.transfer.symlink.impl</code> must be set to
 * value <code>Symlink</code>.
 *
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class Symlink extends Transfer {


    /**
     * The transformation namespace for the transfer job.
     */
    public static final String TRANSFORMATION_NAMESPACE = "pegasus";

    /**
     * The name of the underlying transformation that is queried for in the
     * Transformation Catalog.
     */
    public static final String TRANSFORMATION_NAME = "symlink";

    /**
     * The version number for the transfer job.
     */
    public static final String TRANSFORMATION_VERSION = null;

    /**
     * The derivation namespace for for the transfer job.
     */
    public static final String DERIVATION_NAMESPACE = "pegasus";

    /**
     * The name of the underlying derivation.
     */
    public static final String DERIVATION_NAME = "symlink";

    /**
     * The derivation version number for the transfer job.
     */
    public static final String DERIVATION_VERSION = "1.0";


    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param bag   the bag of initialization objects.
     */
    public Symlink( PegasusBag bag ){
        super( bag );
        
    }

    /**
     * Return a boolean indicating whether the transfers to be done always in
     * a third party transfer mode. A value of false, results in the
     * direct or peer to peer transfers being done.
     * <p>
     * A value of false does not preclude third party transfers. They still can
     * be done, by setting the property "pegasus.transfer.*.thirdparty.sites".
     *
     * @return false always
     *
     */
    public boolean useThirdPartyTransferAlways(){
        return false;
    }

    

   


    /**
     * It constructs the arguments to the transfer executable that need to be passed
     * to the executable referred to in this transfer mode.
     *
     * @param job   the object containing the transfer node.
     * @return  the argument string
     */
    protected String generateArgumentString(TransferJob job) {
        StringBuffer sb = new StringBuffer();
        if(job.vdsNS.containsKey(VDS.TRANSFER_ARGUMENTS_KEY)){
            sb.append(
                      job.vdsNS.removeKey(VDS.TRANSFER_ARGUMENTS_KEY)
                      );
        }

        return sb.toString();
    }
    
  

    /**
     * Writes to a FileWriter stream the stdin which goes into the magic script
     * via standard input
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
            //we want to leverage multiple dests if possible
            NameValue dest   = ft.getDestURL( true );
            writer.write("#" + source.getKey() + "\n" + source.getValue() + "\n#" +
                        dest.getKey() + "\n" + dest.getValue() + "\n");
            writer.flush();
        }


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
    @Override
    public TransformationCatalogEntry getTransformationCatalogEntry(String siteHandle){
        List tcentries = null;
        try {
            //namespace and version are null for time being
            tcentries = mTCHandle.getTCEntries( Symlink.TRANSFORMATION_NAMESPACE,
                                                Symlink.TRANSFORMATION_NAME,
                                                Symlink.TRANSFORMATION_VERSION,
                                                siteHandle,
                                                TCType.INSTALLED);
        } catch (Exception e) {
            mLogger.log(
                "Unable to retrieve entry from TC for " + getCompleteTCName()
                + " Cause:" + e, LogManager.DEBUG_MESSAGE_LEVEL );
        }

        return ( tcentries == null ) ?
                 this.defaultTCEntry( Symlink.TRANSFORMATION_NAMESPACE,
                                      Symlink.TRANSFORMATION_NAME,
                                      Symlink.TRANSFORMATION_VERSION,
                                      siteHandle ): //try using a default one
                 (TransformationCatalogEntry) tcentries.get(0);



    }


    /**
     * Returns the environment profiles that are required for the default
     * entry to sensibly work. The symlink executable does not require any
     * environment variables. Hence an empty implementation that unsets the
     * environment variables required for the Transfer executable.
     *
     * @param site the site where the job is going to run.
     *
     * @return List of environment variables, else null in case where the
     *         required environment variables could not be found.
     */
    protected List getEnvironmentVariables( String site ){
        return new ArrayList() ;
    }

    /**
     * Returns the complete name for the transformation.
     *
     * @return the complete name.
     */
    protected String getCompleteTCName(){
        return Separator.combine(Symlink.TRANSFORMATION_NAMESPACE,
                                 Symlink.TRANSFORMATION_NAME,
                                 this.TRANSFORMATION_VERSION);
    }

    /**
     * Returns the namespace of the derivation that this implementation
     * refers to.
     *
     * @return the namespace of the derivation.
     */
    protected String getDerivationNamespace(){
        return Symlink.DERIVATION_NAMESPACE;
    }


    /**
     * Returns the logical name of the derivation that this implementation
     * refers to.
     *
     * @return the name of the derivation.
     */
    protected String getDerivationName(){
        return Symlink.DERIVATION_NAME;
    }

    /**
     * Returns the version of the derivation that this implementation
     * refers to.
     *
     * @return the version of the derivation.
     */
    protected String getDerivationVersion(){
        return Symlink.DERIVATION_VERSION;
    }
}
