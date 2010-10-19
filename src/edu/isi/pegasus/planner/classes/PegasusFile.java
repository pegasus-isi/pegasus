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

import java.util.BitSet;

import edu.isi.pegasus.planner.dax.File;

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
        INPUT, OUTPUT, INOUT, NONE
    };


    /**
     * The index of the flags field which when set indicates that the file
     * is to be considered optional.
     */
    public static final int TRANSIENT_OPTIONAL_FLAG = 0;

    /**
     * The index of the flags field which when set indicates that the file is
     * not to be registered in the RLS/ RC.
     */
    public static final int TRANSIENT_REGISTRATION_FLAG = 1;

    /**
     * The number of transient flags. This is the length of the BitSet in the
     * flags fields.
     */
    public static final int NO_OF_TRANSIENT_FLAGS = 2;

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
     * The type denoting that a logical file is an other file.
     */
    public static final int OTHER_FILE = 2;
    
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
     * The default constructor.
     */
    public PegasusFile() {
        super();
        mFlags       = new BitSet(NO_OF_TRANSIENT_FLAGS);
        mLogicalFile = new String();
        //by default the type is DATA
        //and transfers are mandatory
        mType        = DATA_FILE;
        mTransferFlag= this.TRANSFER_MANDATORY;
        mSize        = -1;
        mLink        = LINKAGE.NONE;
    }

    /**
     * The overloaded constructor.
     *
     * @param logName  the logical name of the file.
     */
    public PegasusFile(String logName) {
        super();
        mFlags       = new BitSet(NO_OF_TRANSIENT_FLAGS);
        mLogicalFile = logName;
        //by default the type is DATA
        //and transfers are mandatory
        mType        = DATA_FILE;
        mTransferFlag= this.TRANSFER_MANDATORY;
        mSize        = -1;
        mLink        = LINKAGE.NONE;
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
     */
    public void setType(int type) throws IllegalArgumentException{

        if(typeValid(type)){
            mType = type;
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
            mType = PegasusFile.DATA_FILE;
        }
        else if( type.equals( PegasusFile.EXECUTABLE_TYPE )){
            mType = PegasusFile.EXECUTABLE_FILE;
        }
        else if( type.equals( PegasusFile.OTHER_TYPE )){
            mType = PegasusFile.OTHER_FILE;
        }
        else{
            throw new IllegalArgumentException( "Invalid Type passed " + type );
        }
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
        mFlags.set(TRANSIENT_REGISTRATION_FLAG);
    }

    
    /**
     * Sets the transient registration flag to value specified.
     * 
     * @param value the value to set to
     */
    public void setRegisterFlag( boolean value ){
        mFlags.set( TRANSIENT_REGISTRATION_FLAG, !value );
    }
    
    /**
     * Sets the optionalflag denoting the file to be optional to true.
     */
    public void setFileOptional(){
        mFlags.set(TRANSIENT_OPTIONAL_FLAG);
    }

    /**
     * Returns optionalflag denoting the file to be optional or not.
     *
     * @return true  denoting the file is optional.
     *         false denoting that file is not optional.
     */
    public boolean fileOptional(){
        return mFlags.get(TRANSIENT_OPTIONAL_FLAG);
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
     * Returns the transient registration flag (the value of dontRegister).
     *
     * @return true denoting the file need not be registered into the replica
     *              catalog.
     *         false denoting that file needs to be registered.
     */
    public boolean getTransientRegFlag(){
        return mFlags.get(TRANSIENT_REGISTRATION_FLAG);
    }

    /**
     * Returns the bit fields that contain the transient flags (dR and optional).
     *
     *
     * @see #NO_OF_TRANSIENT_FLAGS
     * @see #TRANSIENT_OPTIONAL_FLAG
     * @see #TRANSIENT_REGISTRATION_FLAG
     */
    public BitSet getFlags(){
        return mFlags;
    }

    /**
     * Checks if an object is similar to the one referred to by this class.
     * We compare the primary key to determine if it is the same or not.
     *
     * @return true if the primary key (lfn,transfer flag,transient flag) match.
     *         else false.
     */
/*    public boolean equals(Object o){
        if(o instanceof PegasusFile){
            PegasusFile file = (PegasusFile) o;

            return (file.mLogicalFile.equals(this.mLogicalFile) &&
                    (file.getTransientRegFlag() == this.getTransientRegFlag()) &&
                    (file.getTransferFlag() == this.getTransferFlag()));
        }
        return false;
    }
*/

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
     * Returns a copy of the existing data object.
     *
     * @return clone of the object.
     */
    public Object clone(){
        PegasusFile pf   = new PegasusFile();
        pf.mLogicalFile  = new String(mLogicalFile);
        pf.mFlags        = (BitSet)this.mFlags.clone();
        pf.mType         = mType;
        pf.mTransferFlag = mTransferFlag;
        pf.mSize         = mSize;
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
        String st = "\n Logical Name :" + this.mLogicalFile +
                    "\n Type         :" + typeToString() +
                    "\n Size         :" + mSize +
                    "\n Transient Flags (transfer,optional,dontRegister):" +
                    " ( ";

        st += getTransferFlag() + ",";

        for(int i = 0; i < NO_OF_TRANSIENT_FLAGS; i ++) {
            st += mFlags.get(i) ;
            if( i < NO_OF_TRANSIENT_FLAGS)
                st += ",";
        }
        st += ")";

        return st;
    }

}
