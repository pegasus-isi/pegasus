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

package edu.isi.pegasus.planner.transfer.implementation;

import org.griphyn.cPlanner.classes.TransferJob;
import org.griphyn.cPlanner.classes.NameValue;
import org.griphyn.cPlanner.classes.FileTransfer;

import org.griphyn.cPlanner.classes.PegasusBag;

import edu.isi.pegasus.planner.namespace.Pegasus;

import java.io.FileWriter;
import java.io.File;

import java.util.Collection;
import java.util.Iterator;

/**
 * The implementation that creates transfer jobs referring to the transfer
 * executable distributed with Pegasus.
 *
 * This is an implementation that is differs from Transfer in the way how it
 * invokes the transfer exectuable distributed with Pegasus
 * 
 * It differs in the following ways
 *      - It does not use stdin for transfer, but rather specifies the LOF file 
 *        as an argument on the commandline.
 *      - always executes in the third party mode.
 * <p>
 * In order to use the transfer implementation implemented by this class, the
 * property <code>pegasus.transfer.*.impl</code> must be set to
 * value <code>TPTTransfer</code>.
 *
 * @author Karan Vahi
 * @version $Revision: 907 $
 */
public class TPTTransfer extends Transfer {




    /**
     * The overloaded constructor, that is called by the Factory to load the
     * class.
     *
     * @param bag   the bag of initialization objects.
     */
    public TPTTransfer( PegasusBag bag ){
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
     * @return true always
     *
     */
    public boolean useThirdPartyTransferAlways(){
        return true;
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
        if(job.vdsNS.containsKey(Pegasus.TRANSFER_ARGUMENTS_KEY)){
            sb.append(
                      job.vdsNS.removeKey(Pegasus.TRANSFER_ARGUMENTS_KEY)
                      );
        }
        else{
            sb.append(" -P ").append(mNumOfTXProcesses).
                append(" -p ").append(mNumOfTXStreams);

            sb = (this.mUseForce)?
                sb.append(" -f ") :
                sb;
        }

        sb.append(" base-uri se-mount-point");
        //specify the name of the stdin file on command line
        //since this transfer mode only executes on submit node
        //we can give full path to the stdin
        File f = new File( mPOptions.getSubmitDirectory(), job.getStdIn() );
        sb.append( " " ).append( f.getAbsolutePath() );

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
       /*File f = new File( mPOptions.getSubmitDirectory(), job.getStdIn() );
       //add condor key transfer_input_files to transfer the file
       job.condorVariables.addIPFileForTransfer( f.getAbsolutePath() );
        */
       job.setStdIn( "" );
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

    
}
