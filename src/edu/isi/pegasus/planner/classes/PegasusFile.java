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


package edu.isi.pegasus.planner.classes;

import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import java.util.BitSet;

import edu.isi.pegasus.planner.namespace.Metadata;
import java.util.Iterator;

/**
 * The logical file object that contains the logical filename which is got from
 * the DAX, and the associated set of flags specifying the transient
 * characteristics.
 * It ends up associating the following information with a lfn
 *     -type of the file (data or executable)
 *     -optionality of a file
 *     -transient attributes of a file (dontTransfer and dontRegister)
 *
 * @author Gaurang Mehta
 * @author Karan Vahi
 * @version $Revision$
 */
public class PegasusFile extends Data {

    
    /**
     * Enumeration for denoting type of linkage
     */
    public static enum LINKAGE {
        input, output, inout, none
    };


    /**
     * The index of the flags field which when set indicates that the file
     * is to be considered optional.
     */
    public static final int OPTIONAL_BIT_FLAG = 0;

    /**
     * The index of the flags field which when set indicates that the file is
     * not to be registered in the RLS/ RC.
     */
    public static final int DO_NOT_REGISTER_BIT_FLAG = 1;
    
    /**
     * If set, means can be considered for cleanup  
     */
    public static final int CLEANUP_BIT_FLAG = 2;
    
    /**
     * The number of transient flags. This is the length of the BitSet in the
     * flags fields.
     */
    public static final int NO_OF_TRANSIENT_FLAGS = 3;

    /**
     * The mode where the transfer for this file to the pool
     * is constructed and the transfer job fails if the transfer fails.
     * The corresponding dT (dontTransfer) value is false.
     */
    public static final int TRANSFER_MANDATORY = 0;

    /**
     * The mode where the transfer for this file to the pool is constructed,
     * but the transfer job should not fail if the transfer fails.
     * The corresponding dT (dontTransfer) value is optional.
     */
    public static final int TRANSFER_OPTIONAL = 1;

    /**
     * The mode where the transfer for this file is not constructed.
     * The corresponding dT (dontTransfer) value is true.
     */
    public static final int TRANSFER_NOT = 2;

    /**
     * The string value of a file that is of type data.
     * @see #DATA_FILE
     */
    public static final String DATA_TYPE = "data";

    /**
     * The string value of a file that is of type executable.
     * @see #DATA_FILE
     */
    public static final String EXECUTABLE_TYPE = "executable";
    
    /**
     * The string value of a file that is of type checkpoint file.
     * @see #DATA_FILE
     */
    public static final String CHECKPOINT_TYPE = "checkpoint";
    
    /**
     * The string value of a file that is of type docker container
     * @see DOCKER_CONTAINER#DOCKER_CONTAINER_FILE
     */
    public static final String DOCKER_TYPE = "docker";
    
    /**
     * The string value of a file that is of type docker container
     * @see SINGULARITY_CONTAINER#SINGULARITY_CONTAINER_FILE
     */
    public static final String SINGULARITY_TYPE = "singularity";
    
    
    /**
     * The string value of a file that is of type other.
     * @see #OTHER_FILE
     */
    public static final String OTHER_TYPE = "other";

    /**
     * The type denoting that a logical file is a data file.
     */
    public static final int DATA_FILE = 0;

    /**
     * The type denoting that a logical file is a executable file.
     */
    public static final int EXECUTABLE_FILE = 1;

    /**
     * The type denoting that a logical file is a checkpoint file.
     */
    public static final int CHECKPOINT_FILE = 2;
    
    /**
     * The type denoting that a logical file is a docker container file.
     */
    public static final int DOCKER_CONTAINER_FILE = 3;
    
    /**
     * The type denoting that a logical file is a singularity container file.
     */
    public static final int SINGULARITY_CONTAINER_FILE = 4;
    
    /**
     * The type denoting that a logical file is an other file.
     */
    public static final int OTHER_FILE = 5;
    
    /**
     * The logical name of the file.
     */
    protected String mLogicalFile;

    /**
     * The type associated with the file. It can either be a data file or an
     * executable file.
     *
     * @see #DATA_FILE
     * @see #EXECUTABLE_FILE
     * @see #OTHER_FILE
     * @see #CHECKPOINT_FILE
     */
    protected int mType;

    /**
     * Linkage of the file. Only used for parsers
     */
    protected LINKAGE mLink;

    /**
     * The transfer flag associated with the file containing tristate of
     * transfer,dontTransfer and optional.
     *
     * @see #TRANSFER_MANDATORY
     * @see #TRANSFER_OPTIONAL
     * @see #TRANSFER_NOT
     */
    protected int mTransferFlag;

    /**
     * The transient flags field which is kept as a bit field. It keeps track
     * of the dontRegister and optional attributes associated with the filename
     * in the dax.
     */
    protected BitSet mFlags;
    
    /**
     * The size of the file.
     */
    protected double mSize;

    /**
     * Metadata attributes associated with the file.
     */
    protected Metadata mMetadata;
    
    /**
     * Boolean indicating whether a file is raw input for the wf/fetched from the
     * RC.
     */
    protected boolean mIsRawInput;
    
    /**
     * Boolean indicating whether file checksum is computed during workflow
     * execution either by pegasus-transfer or by some jobs.
     */
    protected boolean mChecksumComputedInWF;
    

    /**
     * The default constructor.
     */
    public PegasusFile() {
        super();
        mFlags       = new BitSet(NO_OF_TRANSIENT_FLAGS);
        //by default files are eligible for cleanup
        mFlags.set( PegasusFile.CLEANUP_BIT_FLAG );
        
        mLogicalFile = "";
        //by default the type is DATA
        //and transfers are mandatory
        mType        = DATA_FILE;
        mTransferFlag= this.TRANSFER_MANDATORY;
        mSize        = -1;
        mLink        = LINKAGE.none;
        mMetadata    = new Metadata();
        mIsRawInput  = false;
        mChecksumComputedInWF = false;
    }

    /**
     * The overloaded constructor.
     *
     * @param lfn  the logical name of the file.
     */
    public PegasusFile(String lfn) {
        this();
        mLogicalFile = lfn;
    }

    /**
     * Sets the linkage for the file during parsing.
     * 
     * @param link  linkage type
     */
    public  void setLinkage( LINKAGE link ){
        mLink = link;
    }


    /**
     * Returns the linkage for the file during parsing.
     *
     * @return the linkage
     */
    public  LINKAGE getLinkage(){
        return mLink;
    }

    /**
     * It returns the lfn of the file that is associated with this transfer.
     *
     * @return the lfn associated with the transfer
     */
    public String getLFN(){
        return this.mLogicalFile;
    }

    /**
     * It sets the logical filename of the file that is being transferred.
     *
     * @param lfn  the logical name of the file that this transfer is associated
     *             with.
     */
    public void setLFN(String lfn){
        mLogicalFile = lfn;
    }

    /**
     * Sets the size for the file.
     *
     * @param size  the size of the file.
     */
    public void setSize( double size ) {
        mSize = size;
    }

    /**
     * Sets the size for the file.
     * 
     * @param size  the size of the file.
     */
    public void setSize( String size ) {
        if( size == null ){
            mSize = -1;
        }
        else{
            mSize = Double.parseDouble( size );
        }
    }

    /**
     * Returns the size for the file. Can be -1 if not set.
     * 
     * @return size if set else -1.
     */
    public double getSize(){
        return mSize;
    }
    
    /**
     * Returns whether the type of file value is valid or not.
     *
     * @param type  the value for the type of file.
     *
     * @return true if the value is in range.
     *         false if the value is not in range.
     */
    public boolean typeValid(int type){
        return (type >= PegasusFile.DATA_FILE &&
                type <= PegasusFile.OTHER_FILE );
    }

    /**
     * Returns whether the transfer value for the mode is in range or not.
     *
     * @param transfer  the value for the transfer.
     *
     * @return true if the value is in range.
     *         false if the value is not in range.
     */
    public boolean transferInRange(int transfer){
        return (transfer >= PegasusFile.TRANSFER_MANDATORY &&
                transfer <= PegasusFile.TRANSFER_NOT);
    }

    /**
     * Sets the type flag to value passed.
     *
     * @param type valid transfer value.
     * @exception IllegalArgumentException if the transfer mode is outside
     * its legal range.
     *
     * @see #DATA_FILE
     * @see #EXECUTABLE_FILE
     * @see #CHECKPOINT_FILE
     */
    public void setType(int type) throws IllegalArgumentException{

        if(typeValid(type)){
            mType = type;
            
            if( mType == PegasusFile.CHECKPOINT_FILE ){
                //a checkpoint file is also an optional file
                this.setFileOptional();
            }
        }
        else{
            throw new IllegalArgumentException();
        }
    }

    /**
     * Sets the transient transfer flag to value passed.
     *
     * @param type valid transfer value.
     * @exception IllegalArgumentException if the transfer mode is outside
     * its legal range.
     *
     * @see #DATA_FILE
     * @see #EXECUTABLE_FILE
     */
    public void setType( String type) throws IllegalArgumentException{

        if( type == null || type.length() == 0)
            throw new IllegalArgumentException( "Invalid Type passed " + type );

        if( type.equals( PegasusFile.DATA_TYPE )){
            setType( PegasusFile.DATA_FILE );
        }
        else if( type.equals( PegasusFile.EXECUTABLE_TYPE )){
            setType( PegasusFile.EXECUTABLE_FILE );
        }
        else if( type.equals( PegasusFile.CHECKPOINT_TYPE )){
            setType( PegasusFile.CHECKPOINT_FILE );
        }
        else if( type.equals( PegasusFile.DOCKER_TYPE )){
            setType(PegasusFile.DOCKER_CONTAINER_FILE );
        }
        else if( type.equals( PegasusFile.SINGULARITY_TYPE )){
            setType(PegasusFile.SINGULARITY_CONTAINER_FILE );
        }
        else if( type.equals( PegasusFile.OTHER_TYPE )){
            setType( PegasusFile.OTHER_FILE );
        }
        else{
            throw new IllegalArgumentException( "Invalid Type passed " + type );
        }
    }
    
    /**
     * Sets the type flag to value passed.
     *
     * @param type valid type of container file
     * @exception IllegalArgumentException if the transfer mode is outside
     * its legal range.
     *
     * @see #DOCKER_CONTAINER_FILE
     * @see #SINGULARITY_CONTAINER_FILE
     */
    public void setType( Container.TYPE type) throws IllegalArgumentException{
        
        switch (type){
            case docker:
                setType( DOCKER_TYPE );
                break;
                
            case singularity:
                setType( SINGULARITY_TYPE );
                break;
                
            default:
                throw new IllegalArgumentException( "Invalid Type passed " + type );
        }
                
                 
    }

    
    /**
     * Sets the flag denoting  that file is a raw input file that is fetched
     * from the RC
     * 
     * @param raw boolean parameter indicating whether file is raw input or not.
     */
    public void setRawInput( boolean raw ){
        this.mIsRawInput = true;
    }

    /**
     * Sets the flag denoting  that file needs to be checksummed during workflow
     * execution or not.
     * 
     * @param checksum  whether to generate checksum or not
     */
    public void setChecksumComputedInWF( boolean checksum ){
        this.mChecksumComputedInWF = checksum;
    }

    /**
     * Sets the transient transfer flag to value passed.
     *
     * @param transfer valid transfer value.
     * @exception IllegalArgumentException if the transfer mode is outside
     * its legal range.
     *
     * @see #TRANSFER_MANDATORY
     * @see #TRANSFER_NOT
     * @see #TRANSFER_OPTIONAL
     */
    public void setTransferFlag(int transfer) throws IllegalArgumentException{

        if(this.transferInRange(transfer)){
            mTransferFlag = transfer;
        }
        else{
            throw new IllegalArgumentException();
        }
    }

    /**
     * Sets the transient transfer flag corresponding to the string
     * value of transfer mode passed. The legal range of transfer values is
     * true|false|optional.
     *
     * @param flag            tri-state transfer value as got from dontTransfer flag.
     *
     * @exception IllegalArgumentException if the transfer mode is outside
     * its legal range.
     *
     * @see #TRANSFER_MANDATORY
     * @see #TRANSFER_NOT
     * @see #TRANSFER_OPTIONAL
     */
    public void setTransferFlag( String flag  ) throws IllegalArgumentException{
        this.setTransferFlag( flag, false );
    }

    /**
     * Sets the transient transfer flag corresponding to the string
     * value of transfer mode passed. The legal range of transfer values is
     * true|false|optional.
     *
     * @param flag            tri-state transfer value as got from dontTransfer flag.
     * @param doubleNegative  indicates whether a double negative or not.
     *
     * @exception IllegalArgumentException if the transfer mode is outside
     * its legal range.
     *
     * @see #TRANSFER_MANDATORY
     * @see #TRANSFER_NOT
     * @see #TRANSFER_OPTIONAL
     */
    public void setTransferFlag( String flag, boolean doubleNegative ) throws IllegalArgumentException{
        if( flag == null || flag.length() == 0){
            //set to default value.
            //throw new IllegalArgumentException();
            mTransferFlag = this.TRANSFER_MANDATORY;
            return;
        }


        if( flag.equals("true") ){
            mTransferFlag = (doubleNegative) ?
                                              this.TRANSFER_NOT :
                                              this.TRANSFER_MANDATORY;
        }
        else if( flag.equals("false")){
            mTransferFlag = ( doubleNegative ) ?
                                              this.TRANSFER_MANDATORY:
                                              this.TRANSFER_NOT;
        }
        else if( flag.equals("optional"))
            mTransferFlag = this.TRANSFER_OPTIONAL;
        else{
            throw new IllegalArgumentException( "Invalid transfer value passed " +
                                                flag );

        }
    }

    /**
     * Returns whether the transfer is transient or not. By transient we mean
     * no transfer.
     *
     * @return  true if transfer mode is TRANSFER_NOT
     *          false if transfer mandatory or optional.
     */
    public boolean getTransientTransferFlag(){
        return (mTransferFlag == this.TRANSFER_NOT);
    }

    /**
     * Sets the transient registration flag to true.
     * 
     * @deprecated
     * @see #setRegisterFlag( boolean )
     */
    public void setTransientRegFlag(){
        mFlags.set(DO_NOT_REGISTER_BIT_FLAG);
    }

    
    /**
     * Sets the transient registration flag to value specified.
     * 
     * @param value the value to set to
     */
    public void setRegisterFlag( boolean value ){
        mFlags.set( DO_NOT_REGISTER_BIT_FLAG, !value );
    }
    
    /**
     * Sets the optional flag denoting the file to be optional to true.
     */
    public void setFileOptional(){
        mFlags.set(OPTIONAL_BIT_FLAG);
    }

    /**
     * Returns optional flag denoting the file to be optional or not.
     *
     * @return true  denoting the file is optional.
     *         false denoting that file is not optional.
     */
    public boolean fileOptional(){
        return mFlags.get(OPTIONAL_BIT_FLAG);
    }
    
    /**
     * Sets the cleanup flag denoting the file can be cleaned up to true.
     */
    public void setForCleanup(){
        mFlags.set(CLEANUP_BIT_FLAG);
    }
    
    /**
     * Sets the cleanup flag to the value passed
     * 
     * @param value the boolean value to which the flag should be set to.
     */
    public void setForCleanup( boolean value ){
        mFlags.set(CLEANUP_BIT_FLAG, value );
    }

    /**
     * Returns cleanup denoting whether the file can be cleaned up or not
     *
     * @return true  denoting the file can be cleaned up.
     */
    public boolean canBeCleanedup(){
        return mFlags.get(CLEANUP_BIT_FLAG);
    }
    

    /**
     * Returns the tristate transfer mode that is associated with the file.
     *
     * @return the int value denoting the type.
     *
     * @see #DATA_FILE
     * @see #EXECUTABLE_FILE
     * @see #OTHER_FILE
     */
    public int getType(){
        return mType;
    }

    /**
     * Returns the tristate transfer mode that is associated with the file.
     *
     * @return the int value denoting the tristate.
     *
     * @see #TRANSFER_MANDATORY
     * @see #TRANSFER_NOT
     * @see #TRANSFER_OPTIONAL
     */
    public int getTransferFlag(){
        return mTransferFlag;
    }

    /**
     * Returns the value of the register flag
     *
     * @return true denoting the file needs be registered into the replica
     *              catalog.
     *         false denoting that file does not need to be registered.
     */
    public boolean getRegisterFlag(){
        return !mFlags.get(DO_NOT_REGISTER_BIT_FLAG);
    }

    
    /**
     * Returns the transient registration flag (the value of dontRegister).
     *
     * @return true denoting the file need not be registered into the replica
     *              catalog.
     *         false denoting that file needs to be registered.
     */
    public boolean getTransientRegFlag(){
        return mFlags.get(DO_NOT_REGISTER_BIT_FLAG);
    }

    /**
     * Returns the bit fields that contain the transient flags (dR and optional).
     *
     *
     * @see #NO_OF_TRANSIENT_FLAGS
     * @see #OPTIONAL_BIT_FLAG
     * @see #DO_NOT_REGISTER_BIT_FLAG
     */
    public BitSet getFlags(){
        return mFlags;
    }
    
    /**
     * Add all the metadata to the file
     * 
     * @param m 
     */
    public void addMetadata(Metadata m) {
        if( !m.isEmpty() ){
            for( Iterator<String> mit = m.getProfileKeyIterator(); mit.hasNext(); ){
                String key = mit.next();
                this.addMetadata( key, (String)m.get(key));
            }
        }
    }

    /**
     * Add metadata to the object.
     * 
     * @param key
     * @param value 
     */
    public void addMetadata( String key, String value ){
       this.mMetadata.checkKeyInNS( key, value );
    }

    /**
     * Returns metadata attribute for a particular key 
     * 
     * @param key
     * 
     * @return value returned else null if not found
     */
    public String getMetadata( String key  ){
       return (String)mMetadata.get( key );
    }
    
    /**
     * Returns all metadata attributes for the file
     * 
     * @return Metadata 
     */
    public Metadata getAllMetadata( ){
       return this.mMetadata;
    }
    
    /**
     * Sets metadata attributes for the file
     * 
     * @param m Metadata 
     */
    public void setMetadata( Metadata m ){
       this.mMetadata = m;
    }


     /**
      * Checks if an object is similar to the one referred to by this class.
      * We compare the primary key to determine if it is the same or not.
      *
      * @return true if the primary key (lfn) matches.
      *         else false.
      */
     public boolean equals( Object o ){
         if(o instanceof PegasusFile){
             PegasusFile file = (PegasusFile) o;

             return (file.mLogicalFile.equals(this.mLogicalFile));
         }
         return false;
     }

     /**
      * Calculate a hash code value for the object to support hash tables.
      *
      * @return a hash code value for the object.
      */
     public int hashCode() {
         return this.mLogicalFile.hashCode();
     }

    /**
     * Returns a boolean indicating if a file that is being staged is an
     * executable or not (i.e is a data file).
     *
     * @return boolean indicating whether a file is executable or not.
     */
    public boolean isExecutable(){
       return (this.mType == PegasusFile.EXECUTABLE_FILE);
    }


    /**
      * Returns a boolean indicating if a file that is being staged is a 
      * checkpoint file or not.
      *
      * @return boolean indicating whether a file is a checkpoint file or not.
      */
     public boolean isCheckpointFile(){
        return (this.mType == PegasusFile.CHECKPOINT_FILE);
     }
     
     /**
      * Returns a boolean indicating if a file that is being staged is an
      * is a data file
      *
      * @return boolean indicating whether a file is a data file or not.
      */
     public boolean isDataFile(){
        return (this.mType == PegasusFile.DATA_FILE);
     }
     
     
    /**
      * Returns a boolean indicating if a file that is being staged is an
      * is a container or not
      *
      * @return boolean indicating whether a file is a container file or not.
      */
    public boolean isContainerFile() {
       return (this.mType == PegasusFile.DOCKER_CONTAINER_FILE || 
                this.mType == PegasusFile.SINGULARITY_CONTAINER_FILE);
    }
     
     /**
      * Returns a boolean indicating if a file that is being staged is a RAW
      * input file
      *
      * @return boolean  
      */
     public boolean isRawInputFile(){
        return this.mIsRawInput;
     }
     
     /**
      * Returns a boolean indicating if a file checksum is generated by 
      * pegasus-transfer
      *
      * @return boolean  
      */
     public boolean hasChecksumComputedInWF(){
        return this.mChecksumComputedInWF;
     }

    /**
     * Returns a boolean indicating whether there is a checksum associated with
     * the file or not in the Replica Catalog or not beforehand. 
     * 
     * @return 
     */
    public boolean hasRCCheckSum() {
        return this.getAllMetadata().containsKey( Metadata.CHECKSUM_VALUE_KEY );
    }

    /**
     * Returns a copy of the existing data object.
     *
     * @return clone of the object.
     */
    public Object clone(){
        PegasusFile pf   = new PegasusFile();
        pf.mLogicalFile  = mLogicalFile;
        pf.mFlags        = (BitSet)this.mFlags.clone();
        pf.mType         = mType;
        pf.mTransferFlag = mTransferFlag;
        pf.mSize         = mSize;
        pf.mMetadata     = (Metadata) this.mMetadata.clone();
        pf.mIsRawInput   = mIsRawInput;
        pf.mChecksumComputedInWF = this.mChecksumComputedInWF;
        return pf;
    }

    /**
     * Returns the type associated with the logical file.
     *
     * @return type of the file.
     */
    public String typeToString(){
        String result = null;
        
        switch( mType ){
            case DATA_FILE:
                result = DATA_TYPE;
                break;
                
            case EXECUTABLE_FILE:
                result = EXECUTABLE_TYPE;
                break;
                
            case CHECKPOINT_FILE:
                result = CHECKPOINT_TYPE;
                break;
            
            case DOCKER_CONTAINER_FILE:
                result = DOCKER_TYPE;
                break;
                
            case SINGULARITY_CONTAINER_FILE:
                result = SINGULARITY_TYPE;
                break;
                
            case OTHER_FILE:
                result = OTHER_TYPE;
                break;
                    
        }
        return result;
    }


    /**
     * Returns the String version of the data object, which is in human readable
     * form.
     *
     * @return the dump of the data object into a string.
     */
    public  String toString(){
        StringBuffer sb = new StringBuffer();
        sb.append( "\n Logical Name :" ).append( this.mLogicalFile ).
           append( "\n Type         :" ).append( typeToString() ).
           append( "\n Size         :" ).append( mSize ).
           append( "\n Transient Flags (transfer,optional,dontRegister,cleanup):").
           append( " ( " ).append( getTransferFlag() ).append( "," );

        for(int i = 0; i < NO_OF_TRANSIENT_FLAGS; i ++) {
            sb.append( mFlags.get(i) );
            if( i < NO_OF_TRANSIENT_FLAGS - 1 ){
                sb.append( "," );
            }
        }
        sb.append( ")");
        
        sb.append( "metadata").append( this.getAllMetadata() );

        return sb.toString();
    }

}
