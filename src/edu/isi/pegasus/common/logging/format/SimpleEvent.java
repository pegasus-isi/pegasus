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
package edu.isi.pegasus.common.logging.format;

import edu.isi.pegasus.common.logging.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.griphyn.common.util.Currently;

/**
 * A Simple LogEvent implementation that is back by a StringBuffer.
 * 
 * 
 * @author Karan Vahi
 * @version $Revision$
 */
public class SimpleEvent implements Event{
    
    /**
     * This is used to format the time stamp.
     */
    //private static final Currently DATE_FORMATTER = new Currently( "yyyy.MM.dd HH:mm:ss.SSS zzz: " );

    /**
     * The name of the program.
     */
    private String mProgram;
    
    /**
     * The buffer that stores information about the event
     */
    private StringBuffer mEventBuffer;
    
    
    /**
     * The buffer that stores information about the log message
     */
    private StringBuffer mLogBuffer;
    
        
        
    /**
     * The default constructor.
     */
    public SimpleEvent(){
        reset();
    }
                
    /**
     * Sets the program name for the software whose log statement are logged.
     * 
     * @param name 
     */
    public void setProgramName( String name ){
        mProgram = name;
    }
    
    
    /**
     * Returns the program name for the software whose log statement are logged.
     * 
     * @param name 
     */
    public String getProgramName( String name ){
        return mProgram;
    }
    
    /**
     * Set the event that is to be associated with the log messages.
     * 
     * @param name        the name of the event to be associated
     * @param entityName  the primary entity that is associated with the event e.g. workflow
     * @param entityID    the id of that entity.
     */
    public void setEvent( String name, String entityName, String entityID ){
        mEventBuffer = new StringBuffer();
        mEventBuffer.append( name ).append( " " ).
                     append( entityName ).append( " " ).
                     append( entityID ).append( " " );
    }
    
    /**
     * Adds the event that is to be associated with the log messages onto an
     * internal stack
     * 
     * @param name  the name of the event to be associated
     * @param map   Map of Entity Names with the entity identifiers.
     */
    public void setEvent( String name, Map<String,String> map ){
        mEventBuffer = new StringBuffer();
        mEventBuffer.append( name ).append( " " );
        
       for( String key : map.keySet()  ){
            mEventBuffer.append( key ).append( " " ).
                         append( map.get(key) ).append( " " );
       }
    }
    
    
    
    /**
     * Returns the name of event that is currently associated with the log messages.
     * 
     * @return  the event to be associated
     */
    public String getEventName( ){
        return null;
    }
    
    /**
     * Creates the start message for the event.
     * 
     * @return start event message
     */
    public String getStartEventMessage(){
        StringBuffer message = new StringBuffer();
        message./*append( DATE_FORMATTER.now() ).append( " " ).*/
                append( mEventBuffer.toString() ).
                append( " - STARTED ");
        return message.toString();
    }
            
    
    /**
     * Creates the end message for the event.
     * 
     * @return end event message
     */
    public String getEndEventMessage(){
        StringBuffer message = new StringBuffer();
        message./*append( DATE_FORMATTER.now() ).append( " " ).*/
                append( mEventBuffer ).
                append( " - FINISHED ");
        return message.toString();
    }
    
    
    /**
     * Reset the internal log message buffer associated with the event
     * 
     */
    public void reset(){
        mEventBuffer = new StringBuffer();
        mLogBuffer   = new StringBuffer();
    }

    /**
     * Add to the log message.
     * 
     * @param key 
     * @param value
     * 
     * @return Self-reference, so calls can be chained
     */
    public Event add( String key, String value ){
        mLogBuffer.append( key ).append( " " ).append( value ).append( " " );
        return this;
    }
    
    /**
     * Creates a log message with the contents of the internal log buffer.
     */
    public String createLogMessage(){
        StringBuffer message = new StringBuffer();
        return message./*append( DATE_FORMATTER.now() ).append( " " )*/append( mLogBuffer ).toString();
    }
    
    
    /**
     * Creates a log message with the contents of the internal log buffer.
     * It then resets the buffer before returning the log message
     * 
     * @return 
     */
    public String createLogMessageAndReset(){
        String result = this.createLogMessage();
        mLogBuffer = new StringBuffer();
        return result;
    }
    
    /**
     * Creates a log message that connects the parent entities with the 
     * children. For e.g. can we use to create the log messages connecting the 
     * jobs with the workflow they are part of.
     * 
     * @param parentType   the type of parent entity
     * @param parentID     the id of the parent entity
     * @param childIdType  the type of children entities
     * @param childIDs     Collection of children id's
     * @return
     */
    public String createEntityHierarchyMessage( String parentType,
                                                String parentID,
                                                String childIdType,
                                                Collection<String> childIDs ){
        StringBuffer result = new StringBuffer();
        //result.append( DATE_FORMATTER.now() ).append( " " );
        result.append( parentType ).append( "<" ).append( parentID ).append( ">" ).
               append( " -> ").append( childIdType ).append( "<" );
        for( String child: childIDs ){
            result.append( child ).append( "," );
        }
        result.append( ">" );
        return result.toString();
    }
    
}
