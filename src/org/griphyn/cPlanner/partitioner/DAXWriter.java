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
package org.griphyn.cPlanner.partitioner;

import org.griphyn.cPlanner.common.LogManager;
import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.common.util.DynamicLoader;
import org.griphyn.common.util.FactoryException;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

/**
 * The abstract class that identifies the interface for writing out a dax
 * corresponding to a partition. The interface stipulates that the jobs making
 * up the partition and relations between those jobs in the partition are
 * identified when invoking it. However all the job details are to be gotten
 * by the implementing classes by parsing the original dax.
 *
 * @author Karan Vahi
 * @version $Revision: 1.8 $
 */
public abstract class DAXWriter {

    /**
     * The prefix added to the name of the dax to identify it is a partitioned
     * dax.
     */
    public static final String PARTITION_PREFIX = "partition_";


    /**
     * The name of the package in which the writers are implemented.
     */
    public static final String PACKAGE_NAME = "org.griphyn.cPlanner.partitioner";

    /**
     * The dax file that is being partitioned. The dax file is the repository
     * for all the jobs in the partitioned daxes.
     */
    protected String mDaxFile;

    /**
     * The directory in which the daxes corresponding to the partition are
     * generated.
     */
    protected String mPDAXDirectory;

    /**
     * The name of the partition dax that are generated.
     */
    protected String mPartitionName;


    /**
     * The handle to the logging object.
     */
    protected LogManager mLogger;

    /**
     * The write handle to the xml file being written.
     */
    protected PrintWriter mWriteHandle;


    /**
     * The default constructor
     */
    protected DAXWriter(){
        mDaxFile = null;
        mPDAXDirectory = null;
        mLogger  = LogManager.getInstance();
        mPartitionName = null;

    }

    /**
     * The overloaded constructor.
     *
     * @param daxFile   the path to the dax file that is being partitioned.
     * @param directory the directory in which the partitioned daxes are to be
     *                  generated.
     */
    protected DAXWriter(String daxFile, String directory) {
        mLogger  = LogManager.getInstance();
        mDaxFile = daxFile;
        mPDAXDirectory = directory;
        mPartitionName = null;
    }

    /**
     * It writes out a dax consisting of the jobs as specified in the partition.
     *
     * @param partition  the partition object containing the relations and id's
     *                   of the jobs making up the partition.
     *
     * @return boolean  true if dax successfully generated and written.
     *                  false in case of error.
     */
    public boolean writePartitionDax( Partition partition ){
        return writePartitionDax( partition, partition.getIndex() );
    }



    /**
     * It writes out a dax consisting of the jobs as specified in the partition.
     *
     * @param partition  the partition object containing the relations and id's
     *                   of the jobs making up the partition.
     * @param index      the index of the partition.
     *
     * @return boolean  true if dax successfully generated and written.
     *                  false in case of error.
     */
    public abstract boolean writePartitionDax( Partition partition, int index );



    /**
     * The ends up loading the PDAXWriter.  It selects the writer as specified by
     * the vds.partition.parse.mode property.
     *
     * @param properties the handle to the properties visible to Pegasus.
     * @param daxFile    the path to the dax file that is being partitioned.
     * @param directory  the directory in which the partitioned daxes are to be
     *                   generated.
     */
    public static DAXWriter loadInstance( PegasusProperties properties,
                                          String daxFile,
                                          String directory) {

        String className = properties.getPartitionParsingMode();
        className = (className.equalsIgnoreCase("single"))?"SingleLook":
                     (className.equalsIgnoreCase("multiple"))?"MultipleLook":
                      className;

        return loadInstance( className, properties, daxFile, directory );
    }





    /**
     * Loads the implementing PDAXWriter. The name of the class that is to be
     * loaded is passed and can be complete(with package name) or just the name
     * of the class, in which case the class is loaded from the default package.
     *
     * @param properties the handle to the properties visible to Pegasus.
     * @param className  the name of the class with or without the package name.
     * @param daxFile   the path to the dax file that is being partitioned.
     * @param directory the directory in which the partitioned daxes are to be
     *                  generated.
     *
     * @throws FactoryException that nests any error that
     *         might occur during the instantiation of the implementation.
     */
    public static DAXWriter loadInstance( String className,
                                          PegasusProperties properties,
                                          String daxFile,
                                          String directory) throws FactoryException{

        if(className.indexOf('.') == -1){
            //prepend the default package name
            className = PACKAGE_NAME + "." + className;
        }

        //sanity and default checks
        directory = (directory == null)? "." : directory;

        //try loading the class dynamically
        DAXWriter writer = null;
        DynamicLoader dl = new DynamicLoader( className);
        try {
               Object argList[] = new Object[2];
               argList[0] = daxFile;
               argList[1] = directory;
               writer = (DAXWriter) dl.instantiate(argList);
        }
        catch ( Exception e ) {
            throw new FactoryException( "Instantiating DAXWriter",
                                        className,
                                        e );
        }

        return writer;

    }


    /**
    * It constructs the name of the partitioned dax file that has to be written
    * corresponding to a partition of the dax. The dax name returned has no
    * prefix added to it.
    *
    * @param daxName  the name attribute in the adag element of the dax.
    * @param index     the partition number of the partition.
    */
   public static String getPDAXFilename(String daxName, int index){
       return getPDAXFilename(daxName,index,false);
   }



   /**
    * It constructs the name of the partitioned dax file that has to be written
    * corresponding to a partition of the dax.
    *
    * @param daxName  the name attribute in the adag element of the dax.
    * @param index     the partition number of the partition.
    * @param addPrefix whether you want to addPrefix or not.
    */
   public static String getPDAXFilename(String daxName, int index,
                                        boolean addPrefix){
       StringBuffer sb = new StringBuffer(32);

       //get the partition name
       sb.append(constructPartitionName(daxName,addPrefix));

       //add the suffix
       sb.append("_").append(index).append(".dax");

       return sb.toString();
   }


   /**
    * It constructs the partition name given the daxName. It only ends up adding
    * the prefix if the addPrefix parameter is set.
    *
    * @param daxName    the name attribute in the adag element of the dax.
    * @param addPrefix  whether to add prefix or not.
    */
   private static String constructPartitionName(String daxName, boolean addPrefix){
       StringBuffer sb = new StringBuffer();

       //append the partition prefix to it.
       if(addPrefix)
           sb.append(PARTITION_PREFIX);

       //construct a partition name
       sb = (daxName == null)?
            // set it to the default name
            sb.append("test") :
            sb.append(daxName);

       return sb.toString();
    }



    /**
     * It sets the name of the partition in the dax that is generated. It suffixes
     * PARTITION_PREFIX to the name of the dax.
     *
     * @param daxName the name attribute in the adag element of the dax.
     */
    public void setPartitionName(String daxName){
        //yes we want the partition prefix to be added
        mPartitionName = constructPartitionName(daxName,true);
    }



    /**
     * It returns the name of the partitioned dax, that the object is
     * currently writing or initialized to write. By the name, one means the
     * value that is set to the name attribute in the adag element.
     */
    public String getPartitionName(){
        return mPartitionName;
    }

    /**
     * This initializes the write handle a file in directory specified
     * when creating the instance of this class. The name of the file is
     * constructed by default, by looking at the partition name that is
     * assigned to the name attribute for the adag element.
     *
     * @param index the partition number of the partition.
     */
    public void initializeWriteHandle(int index){
        //check if partition name is set
        if(mPartitionName == null){
            //set it to default
            setPartitionName(null);
        }
        String name = mPartitionName + "_" + index + ".dax";
        initializeWriteHandle(name);

    }

    /**
     * This initializes the write handle to the file in directory specified
     * when creating the instance of this class.
     *
     * @param fileName  the name of the file that is to be written in the
     *                  directory.
     */
    public void initializeWriteHandle(String fileName){
        String completeName = mPDAXDirectory + File.separator + fileName;
        try{
            //if the write handle was not explicitly closed, closing it
            if(mWriteHandle != null)
                this.close();

            mWriteHandle =
                new PrintWriter(new BufferedWriter(new FileWriter(completeName)));
        }
        catch(IOException e){
            throw new RuntimeException( "Unable to write to file " + completeName + " :",
                                        e);
        }

    }


    /**
     * Writes out to the file.
     */
    public void writeln(String st){
        mWriteHandle.println(st);
    }

    /**
     * Close the write handle to the  file that is written.
     */
    public void close(){
        if( mWriteHandle != null ){
            mWriteHandle.close();
            mWriteHandle = null;
        }
    }

}
