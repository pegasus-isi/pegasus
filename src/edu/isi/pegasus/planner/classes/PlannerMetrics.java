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

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.common.PegasusProperties;

import java.io.File;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.Date;
import java.util.Properties;

/**
 * A Data class containing the metrics about the planning instance.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PlannerMetrics extends Data{
    
    /**
     * The base submit directory where the files are being created.
     */
    private String mBaseSubmitDirectory;

    /**
     * The relative submit directory for this run.
     */
    private String mRelativeSubmitDirectory;


    /*
     * The file to which the metrics should be written out in the submit directory
     */
    private File mMetricsFileInSubmitDirectory;


    /**
     * The path to the DAX that was planned by the workflow.
     */
    private String mDAXPath;

    /**
     * The pointer to the properties file that was used.
     */
    private String mPropertiesPath;

    /**
     * The user who planned the workflow.
     */
    private String mUser;

    /**
     * The VOGroup to which the user belonged to.
     */
    private String mVOGroup;


    /**
     * The number formatter to format the run submit dir entries.
     */
    private NumberFormat mNumFormatter;


    /**
     * The name of the client
     */
    @Expose @SerializedName( "client" ) private  final String mClient = "pegasus-plan";

    /**
     * The planner version
     */
    @Expose @SerializedName( "version" ) private  final String mVersion = new Version().getVersion();

    /**
     * The name of the client
     */
    @Expose @SerializedName( "type" ) private   String mType ;

    /**
     * The start time for the planning.
     */
    @Expose @SerializedName("start_time") private double mStartTime;

    /**
     * The end time for the planning.
     */
    @Expose @SerializedName("end_time") private double mEndTime;

    /**
     * The planning duration
     */
    @Expose @SerializedName("duration") private double mDuration;

    /**
     * the exitcode of the planner
     */
    @Expose @SerializedName("exitcode") private int mExitcode;

    /**
     * The Root Workflow UUID.
     */
    @Expose @SerializedName( "root_wf_uuid" ) private String mRootWorkflowUUID;

    /**
     * The UUID associated with the workflow.
     */
    @Expose @SerializedName( "wf_uuid" ) private String mWorkflowUUID;

    /**
     * The data configuration mode
     */
    @Expose @SerializedName( "data_config" )private String mDataConfiguration;
    
    /**
     * The arguments with which the planner was invoked with
     */
    @Expose @SerializedName( "planner_args" )private String mPlannerArguments;
    
    /**
     * A boolean indicating whether pmc was used or not.
     */
    @Expose @SerializedName( "uses_pmc" )private boolean mUsesPMC;

    /**
     * The metrics about the workflow.
     */
    @Expose @SerializedName("wf_metrics") private WorkflowMetrics mWFMetrics;
    
    /**
     * The application metrics that need to be forwarded
     */
    @Expose @SerializedName( "app_metrics" ) private Properties mApplicationMetrics;

    /**
     * The error message to be logged
     */
    @Expose @SerializedName( "error" ) private String mErrorMessage;
    
    

    /**
     * The default metrics.
     */
    public PlannerMetrics() {
        //the exitcode is explicitly set to -1
        //it should be set when the planner ends with the correct exitcode
        mExitcode = -1;
        mNumFormatter = new DecimalFormat( "#.###" );
        mType = "metrics";
        mPlannerArguments = "";
        mUsesPMC = false;
        //we want metrics to be serialized only if user specified
        //mApplicationMetrics = new Properties();
    }

    /**
     * Returns the UUID for the Root workflow
     *
     * @return the UUID of the workflow
     */
    public String getRootWorkflowUUID() {
        return this.mRootWorkflowUUID;
    }


    /**
     * Sets the root UUID for the workflow
     *
     * @param uuid   the UUID of the workflow
     */
    public void setRootWorkflowUUID( String uuid ) {
        this.mRootWorkflowUUID = uuid;
    }


    /**
     * Returns the UUID for the workflow
     *
     * @return the UUID of the workflow
     */
    public String getWorkflowUUID() {
        return this.mWorkflowUUID;
    }


    /**
     * Sets the UUID for the workflow
     *
     * @param uuid   the UUID of the workflow
     */
    public void setWorkflowUUID( String uuid ) {
        this.mWorkflowUUID = uuid;
    }

    /**
     * Returns the workflow metrics.
     *
     * @return the workflow metrics.
     */
    public WorkflowMetrics getWorkflowMetrics(){
        return mWFMetrics;
    }

    /**
     * Sets the workflow metrics.
     *
     * @param metrics the workflow metrics.
     */
    public void setWorkflowMetrics( WorkflowMetrics metrics ){
        mWFMetrics = metrics;
    }

    /**
     * Sets the app metrics that need to be forwarded.
     * 
     * @param metrics the application metrics
     */
    public void setApplicationMetrics( Properties properties ){
        this.mApplicationMetrics = properties;
    }

    /**
     * Sets the app metrics that need to be forwarded.
     * 
     * @param metrics the application metrics
     * @param dax     the dax
     */
    public void setApplicationMetrics( PegasusProperties properties, String dax ){
        //figure out the application name if set
        String name = properties.getProperty( PegasusProperties.PEGASUS_APP_METRICS_PREFIX );
        if( name == null ){
            //PM-1220 compute from the dax name
            name = new File(dax).getName();
            //try and strip out extension if exists
            int index = name.indexOf( '.' );
            if( index != -1 ){
                name = name.substring( 0, index );
            }
        }
        
        if( name != null ){
            mApplicationMetrics = properties.matchingSubset( PegasusProperties.PEGASUS_APP_METRICS_PREFIX, false );
            //add the name
            mApplicationMetrics.setProperty( "name", name );
        }
        
    }

    /**
     * Returns the application specific metrics that will be forwarded to the
     * server
     * 
     * @return the application metrics
     */
    public Properties getApplicationMetrics( ){
        return this.mApplicationMetrics;
    }

    /**
     * Returns the username.
     *
     * @return the user.
     */
    public String getUser( ){
        return mUser;
    }


    /**
     * Sets the user.
     *
     * @param user  the user.
     */
    public void setUser( String user ){
        mUser = user;
    }

    /**
     * Sets the metrics
     *
     * @param type  the metrics type
     */
    public void setMetricsType( String type ){
        mType = type;
    }

    /**
     * Convenience setter method
     *
     * @param type  the metrics type
     */
    public void setMetricsTypeToError(   ){
        mType = "error";
    }


    /**
     * Returns the metric type
     *
     * @return  metrics type
     */
    public String getMetricsType(   ){
        return mType;
    }


    /**
     * Sets the vo group
     *
     * @param group the vo group.
     */
    public void setVOGroup( String group ){
        this.mVOGroup = group;
    }


    /**
     * Returns the VO Group.
     *
     * @return  the VO Group to which the user belongs
     */
    public String getVOGroup( ){
        return this.mVOGroup;
    }


    /**
     * Sets the path to the properties file for the run.
     *
     * @param path  the path to the properties file.
     */
    public void setProperties( String path ){
        mPropertiesPath = path;
    }

    /**
     * Returns the path to the properties file for the run.
     *
     * @return  the path to the properties file.
     */
    public String getProperties( ){
        return mPropertiesPath;
    }


    /**
     * Sets the path to the base submit directory.
     *
     * @param base  the path to the base submit directory.
     */
    public void setBaseSubmitDirectory( String base ){
        mBaseSubmitDirectory = base;
    }


    /**
     * Returns the path to the base submit directory.
     *
     * @return  the path to the base submit directory.
     */
    public String getBaseSubmitDirectory( ){
        return mBaseSubmitDirectory;
    }

    /**
     * Sets the path to the submit directory relative to the base.
     *
     * @param relative  the relative path from the base submit directory.
     */
    public void setRelativeSubmitDirectory( String relative ){
        mRelativeSubmitDirectory = relative;
    }


    /**
     * Returns the path to the relative submit directory.
     *
     * @return  the path to the relative submit directory.
     */
    public String getRelativeSubmitDirectory( ){
        return mRelativeSubmitDirectory;
    }

    /**
     * Sets the metrics file location in the submit directory
     * 
     * @param f  the file pointing to the metrics file
     */
    public void setMetricsFileLocationInSubmitDirectory( File f ){
        this.mMetricsFileInSubmitDirectory = f;
    }

    /**
     * Sets the metrics file location in the submit directory
     *
     * @return the file pointing to the metrics file. can be null
     */
    public File getMetricsFileLocationInSubmitDirectory(   ){
        return this.mMetricsFileInSubmitDirectory;
    }


    /**
     * Sets the path to the DAX.
     *
     * @param path  the path to the DAX file.
     */
    public void setDAX( String path ){
        mDAXPath = path;
    }

    /**
     * Sets the path to the DAX.
     *
     * @return  the path to the DAX file.
     */
    public String getDAX( ){
        return mDAXPath;
    }


    /**
     * The data configuration
     *
     * @param configuration   the data configuration
     */
    public void setDataConfiguration(String configuration) {
        mDataConfiguration = configuration;
    }

    /**
     * Returns the data configuration
     *
     * @return the data configuration
     */
    public String getDataConfiguration( ) {
        return mDataConfiguration ;
    }

    /**
     * Sets the planner options
     *
     * @param options   options
     */
    public void setPlannerOptions(PlannerOptions options) {
        this.setPlannerOptions( options.getOriginalArgString() );
    }
    
    /**
     *Sets The planner options
     *
     * @param arguments   options
     */
    public void setPlannerOptions(String arguments) {
         mPlannerArguments = arguments;
    }

    /**
     * Returns the planner arguments
     *
     * @return the data configuration
     */
    public String getPlannerArguments( ) {
        return mPlannerArguments ;
    }

    /**
     * Returns boolean indicating whether PMC was used or not
     * 
     * @return 
     */
    public boolean usesPMC() {
        return this.mUsesPMC;
    }

    /**
     * Sets the uses PMC parameter
     * 
     * @param usesPMC 
     */
    public void setUsesPMC(boolean usesPMC) {
        mUsesPMC = usesPMC;
    }

    /**
     * Set the start time for the planning operation.
     *
     * @param start   the start time.
     */
    public void setStartTime( Date start ){
        double t = start.getTime();
        //mStartTime = mNumFormatter.format( t/1000 );
        mStartTime = t/1000;
    }

    /**
     * Set the start time for the planning operation.
     *
     * @param start   the start time.
     */
    public void setStartTime( double start ){
        mStartTime = start;
    }

    /**
     * Returns the start time for the planning operation as epoch with
     * millisecond precision
     *
     * @return   the start time.
     */
    public double getStartTime( ){
        return mStartTime;
    }


    /**
     * Set the end time for the planning operation.
     *
     * @param end   the end time.
     */
    public void setEndTime( Date end ){
        double t = end.getTime();
        //mEndTime = mNumFormatter.format( t/1000 );
        mEndTime   = t/1000;
    }

    /**
     * Set the end time for the planning operation.
     *
     * @param end   the end time.
     */
    public void setEndTime( double end ){
        mEndTime = end;
    }


    /**
     * Returns the end time for the planning operation as epoch with
     * millisecond precision
     *
     * @return   the end time.
     */
    public double getEndTime( ){
        return mEndTime;
    }

    /**
     * Returns the duration for the planning
     *
     * @return the duration
     */
    public double getDuration( ){
        return mDuration;
    }


    /**
     * Sets the user.
     *
     * @param duration  the duration
     */
    public void setDuration( double duration ){
        mDuration = duration;
    }

    /**
     * Returns the exitcode for the planner
     *
     * @return the exitcode
     */
    public int getExitcode( ){
        return mExitcode;
    }


    /**
     * Sets the exitcode for the planner.
     *
     * @param exitcode the exitcode
     */
    public void setExitcode( int exitcode ){
        mExitcode = exitcode;
    }

    /**
     * Set the error message that we want to log
     *
     * @param error   error
     */
    public void setErrorMessage(String error ) {
        mErrorMessage = error;
    }


    /**
     * Returns the error message that we want to log
     *
     * @return  the error message
     */
    public String getErrorMessage( ) {
        return mErrorMessage;
    }

    /**
     * Converts the planner metrics to JSON
     * 
     * @return  the planner metrics in JSON
     */
    public String toJson(){
        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().create();
        return gson.toJson( this );      
    }

    /**
     * Converts the planner metrics to JSON
     * 
     * @return  the planner metrics in JSON
     */
    public String toPrettyJson(){
        //Gson gson = new Gson();
        //Gson gson = new GsonBuilder().setPrettyPrinting().create();
        Gson gson = new GsonBuilder().excludeFieldsWithoutExposeAnnotation().setPrettyPrinting().create();
        return gson.toJson( this );      
    }


    /**
     * Returns a textual description of the object.
     *
     * @return Object
     */
    public String toString(){
        StringBuffer sb = new StringBuffer();

        sb.append( "{" ).append( "\n" );

        append( sb, "client", this.mClient );
        append( sb, "version", this.mVersion );
        append( sb, "user", this.mUser );
        append( sb, "vogroup", this.mVOGroup );
        append( sb, "submitdir.base", this.mBaseSubmitDirectory );
        append( sb, "submitdir.relative", this.mRelativeSubmitDirectory );
        append( sb, "planning.start",   mNumFormatter.format( mStartTime ) );
        append( sb, "planning.end", mNumFormatter.format( mEndTime ) );
        append( sb, "planning.arguments", mPlannerArguments );
        append( sb, "uses_pmc", Boolean.toString(this.usesPMC()) );
        append( sb, "duration" ,  Double.toString( mDuration )  );
        append( sb, "exitcode" ,  Integer.toString( mExitcode )  );
        append( sb, "error" , mErrorMessage );
        append( sb, "properties", this.mPropertiesPath );
        append( sb, "dax", this.mDAXPath );
        append( sb, "data.configuration", this.mDataConfiguration );
        append( sb, "root.wf.uuid", this.mRootWorkflowUUID );
        append( sb, "wf.uuid", this.mWorkflowUUID );
        sb.append( this.getWorkflowMetrics() );
        if( this.mApplicationMetrics != null ){
            append( sb, "app.metrics", this.mApplicationMetrics.toString() );
        }
        sb.append( "}" ).append( "\n" );

        return sb.toString();
    }


    /**
     * Appends a key=value pair to the StringBuffer.
     *
     * @param buffer    the StringBuffer that is to be appended to.
     * @param key   the key.
     * @param value the value.
     */
    protected void append( StringBuffer buffer, String key, String value ){
        buffer.append( key ).append( " = " ).append( value ).append( "\n" );
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone(){
        PlannerMetrics pm;
        try {
            pm = ( PlannerMetrics )super.clone();
        }
        catch (CloneNotSupportedException e) {
            //somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException( "Clone not implemented in the base class of " +
                                        this.getClass().getName(),
                                        e);
        }

        pm.mNumFormatter = (NumberFormat) this.mNumFormatter.clone();
        pm.setUser( this.mUser );
        pm.setVOGroup( this.mVOGroup );
        pm.setBaseSubmitDirectory( this.mBaseSubmitDirectory );
        pm.setRelativeSubmitDirectory( this.mRelativeSubmitDirectory );
        pm.setProperties( this.mPropertiesPath );
        pm.setDAX( this.mDAXPath );
        pm.setDataConfiguration( this.mDataConfiguration );
        pm.setPlannerOptions( this.mPlannerArguments);
        pm.setUsesPMC( this.mUsesPMC );
        pm.setStartTime( this.mStartTime );
        pm.setEndTime( this.mEndTime );
        pm.setDuration( this.mDuration );
        pm.setExitcode( this.mExitcode );
        pm.setErrorMessage( this.mErrorMessage );
        if( this.mApplicationMetrics != null ){
            pm.setApplicationMetrics( (Properties) this.mApplicationMetrics.clone());
        }
        return pm;
    }

    
    


    
}
