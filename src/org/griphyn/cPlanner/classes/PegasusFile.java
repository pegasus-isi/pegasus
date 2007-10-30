/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */

package org.griphyn.cPlanner.classes;

import java.util.BitSet;

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
     * The type denoting that a logical file is a data file.
     */
    public static final int DATA_FILE = 0;

    /**
     * The type denoting that a logical file is a executable file.
     */
    public static final int EXECUTABLE_FILE = 1;

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
     */
    protected int mType;

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
     * Returns whether the type of file value is valid or not.
     *
     * @param type  the value for the type of file.
     *
     * @return true if the value is in range.
     *         false if the value is not in range.
     */
    public boolean typeValid(int type){
        return (type >= this.DATA_FILE &&
                type <= this.EXECUTABLE_FILE);
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
        return (transfer >= this.TRANSFER_MANDATORY &&
                transfer <= this.TRANSFER_NOT);
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

        if( type.equals( this.DATA_TYPE )){
            mType = this.DATA_FILE;
        }
        else if( type.equals( this.EXECUTABLE_TYPE )){
            mType = this.EXECUTABLE_FILE;
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
     */
    public void setTransientRegFlag(){
        mFlags.set(TRANSIENT_REGISTRATION_FLAG);
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
        return pf;
    }

    /**
     * Returns the type associated with the logical file.
     *
     * @return type of the file.
     */
    public String typeToString(){
        return (mType == DATA_FILE)?DATA_TYPE:EXECUTABLE_TYPE;
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
                    "\n Transient Flags (transfer,dontRegister,optional):" +
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
