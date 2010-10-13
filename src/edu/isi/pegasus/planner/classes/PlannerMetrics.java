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

import edu.isi.pegasus.common.util.Currently;

import java.util.Date;

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
     * The metrics about the workflow.
     */
    private WorkflowMetrics mWFMetrics;


    /**
     * The start time for hte planning.
     */
    private Date mStartTime;

    /**
     * The end time for the planning.
     */
    private Date mEndTime;


    /**
     * The default metrics.
     */
    public PlannerMetrics() {


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
     * Returns the username.
     *
     * @return the user.
     */
    public String setUser( ){
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
     * Set the start time for the planning operation.
     *
     * @param start   the start time.
     */
    public void setStartTime( Date start ){
        mStartTime = start;
    }

    /**
     * Returns the start time for the planning operation.
     *
     * @return   the start time.
     */
    public Date getStartTime( ){
        return mStartTime;
    }


    /**
     * Set the end time for the planning operation.
     *
     * @param end   the end time.
     */
    public void setEndTime( Date end ){
        mEndTime = end;
    }

    /**
     * Returns the end time for the planning operation.
     *
     * @return   the end time.
     */
    public Date getEndTime( ){
        return mEndTime;
    }



    /**
     * Returns a textual description of the object.
     *
     * @return Object
     */
    public String toString(){
        StringBuffer sb = new StringBuffer();

        sb.append( "{" ).append( "\n" );

        append( sb, "user", this.mUser );
        append( sb, "vogroup", this.mVOGroup );
        append( sb, "submitdir.base", this.mBaseSubmitDirectory );
        append( sb, "submitdir.relative", this.mRelativeSubmitDirectory );
        append( sb, "planning.start", Currently.iso8601( false, true, false, mStartTime ) );
        append( sb, "planning.end", Currently.iso8601( false, true, false, mEndTime ) );
        append( sb, "properties", this.mPropertiesPath );
        append( sb, "dax", this.mDAXPath );
        sb.append( this.getWorkflowMetrics() );

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

        pm.setUser( this.mUser );
        pm.setVOGroup( this.mVOGroup );
        pm.setBaseSubmitDirectory( this.mBaseSubmitDirectory );
        pm.setRelativeSubmitDirectory( this.mRelativeSubmitDirectory );
        pm.setProperties( this.mPropertiesPath );
        pm.setDAX( this.mDAXPath );
        pm.setStartTime( this.mStartTime );
        pm.setEndTime( this.mEndTime );

        return pm;
    }
}
