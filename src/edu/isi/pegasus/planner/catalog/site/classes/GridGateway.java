/*
 * 
 *   Copyright 2007-2008 University Of Southern California
 * 
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing,
 *   software distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 * 
 */

package edu.isi.pegasus.planner.catalog.site.classes;


import edu.isi.pegasus.planner.catalog.classes.Architecture;
import edu.isi.pegasus.planner.catalog.classes.OS;


import java.io.Writer;
import java.io.IOException;

/**
 * This class describes the Grid Gateway into a site.
 * 
 * @version $Revision$
 * @author Karan Vahi
 */
public class GridGateway extends AbstractSiteData{

    /**
     * An enumeration of valid types of grid gateway.
     */
    public static enum TYPE { gt2, gt4, condor, condorc, pbs, lsf };
    
    /**
     * An enumeration of types of jobs handled by an instance of a grid gateway.
     */
    public static enum JOB_TYPE{ compute, auxillary, transfer, register, cleanup };
    
    
    /**
     * An enumeration of valid schedulers on the grid gateway.
     */
    public static enum SCHEDULER_TYPE{ Fork, LSF, PBS, Condor };
    
    
    /**
     * The grid type associated with this instance.
     */
    private TYPE mType;
    
    /**
     * The contact string for the grid gateway.
     */
    private String mContact;
    
    /**
     * The scheduler type with which it interfaces.
     */
    private SCHEDULER_TYPE mScheduler;
    
    /**
     * The type of jobs that this grid gateway can accept.
     */
    private JOB_TYPE mJobType;
    
    /**
     * An optional os type for the grid gateway.
     */
    private OS mOS;
    
    /**
     * An optional architecture type for the grid gateway.
     */
    private Architecture mArch;
    
    
    /**
     * The overloaded constructor.
     * 
     * @param type          the type of grid gateway.
     * @param contact       the contact string for it.
     * @param scheduler     the underlying scheduler that it talks to.
     */
    public GridGateway( TYPE type, String contact, SCHEDULER_TYPE scheduler ){
        mType = type;
        mContact = contact;
        mScheduler = scheduler;
    }
    
    
    /**
     * Sets the type of jobs that run via this grid gateway.
     * 
     * @param type  the job type.
     */
    public void setJobType( JOB_TYPE type ){
        mJobType = type;
    }
    
    
    /**
     * Returns the type of jobs that run via this grid gateway.
     * 
     * @return  the job type.
     */
    public JOB_TYPE getJobType( ){
        return mJobType;
    }
    
     /**
     * Sets the type of  grid gateway.
     * 
     * @param type  the gateway type.
     */
    public void setType( TYPE type ){
        mType = type;
    }
    
    
    /**
     * Returns the type of jobs that run via this grid gateway.
     * 
     * @return  the type of grid gateway
     */
    public TYPE getType( ){
        return mType;
    }

    /**
     * Sets the contact string for the Grid gateway
     * 
     * @param contact  the contact string
     */
    public void setContact( String contact ){
        mContact = contact;
    }
    
    
    /**
     * Returns the contact string for the Grid gateway
     * 
     * @return  the contact string
     */
    public String getContact( ){
        return mContact;
    }
    
    /**
     * Sets the the underlying scheduler that gateway talks to.
     * 
     * @param scheduler the scheduler.
     */
    public void setScheduler( SCHEDULER_TYPE scheduler ){
        mScheduler = scheduler;
    }
    
    /**
     * Returns the the underlying scheduler that gateway talks to.
     * 
     * @return  the scheduler.
     */
    public SCHEDULER_TYPE getScheduler( ){
        return mScheduler;
    }
    
    
    /**
     * Sets the OS of the nodes that this grid gateway talks to.
     * 
     * @param os   the os
     */
    public void setOS( OS os ){
        mOS = os;
    }

    /**
     * Returns the OS of the nodes that this grid gateway talks to.
     * 
     * @return the os if set else null
     */
    public OS getOS( ){
        return mOS;
    }
    
    /**
     * Sets the architecture of the nodes that this grid gateway talks to.
     * 
     * @param arch   the architecture of the nodes
     */
    public void setArchitecture( Architecture arch ){
        mArch = arch;
    }

    
    /**
     * Returns the architecture of the nodes that this grid gateway talks to.
     * 
     * @return the architecture if set else null
     */
    public Architecture getArchitecture( ){
        return mArch;
    }
    
    /**
     * Writes out the xml description of the object. 
     *
     * @param writer is a Writer opened and ready for writing. This can also
     *               be a StringWriter for efficient output.
     * @param indent the indent to be used.
     *
     * @exception IOException if something fishy happens to the stream.
     */
    public void toXML( Writer writer, String indent ) throws IOException {
        String newLine = System.getProperty( "line.separator", "\r\n" );
        String newIndent = indent + "\t";
        
        //write out the  xml element
        writer.write( indent );
        writer.write( "<grid " );        
        writeAttribute( writer, "type", getType().toString() );        
        writeAttribute( writer, "contact", getContact() );        
        writeAttribute( writer, "scheduler", getScheduler().toString() );
        writeAttribute( writer, "jobtype", getJobType().toString() );
        
        if( mOS != null ){            
            writeAttribute( writer, "os", getOS().toString() );
        }
        if( mArch != null ){            
            writeAttribute( writer, "arch", getArchitecture().toString() );
        }
        
        writer.write( ">");
        writer.write( newLine );
        
        
        writer.write( indent );
        writer.write( "</grid>" );
        writer.write( newLine );
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone(){
        GridGateway obj;
        try{
            obj = ( GridGateway ) super.clone();
            obj.setType( this.getType() );
            obj.setContact( this.getContact() );
            obj.setScheduler( this.getScheduler() );
            obj.setJobType( this.getJobType() );
            obj.setOS( this.getOS() );
            obj.setArchitecture( this.getArchitecture() );
        }
        catch( CloneNotSupportedException e ){
            //somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException("Clone not implemented in the base class of " + this.getClass().getName(),
                                       e );
        }
        return obj;
    }
    
}
