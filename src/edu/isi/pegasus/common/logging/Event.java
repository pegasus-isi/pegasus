/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.common.logging;

/**
 *
 * @author vahi
 */
public interface Event extends Cloneable{
    
    
    /**
     * Sets the program name for the software whose log statement are logged.
     * 
     * @param name 
     */
    public void setProgramName( String name );
    
    
    /**
     * Returns the program name for the software whose log statement are logged.
     * 
     * @param name 
     */
    public String getProgramName( String name );
    
    /**
     * Set the event that is to be associated with the log messages.
     * 
     * @param name        the name of the event to be associated
     * @param entityName  the primary entity that is associated with the event e.g. workflow
     * @param entityID    the id of that entity.
     */
    public void setEvent( String name, String entityName, String entityID );
    
    
    /**
     * Returns the name of event that is currently associated with the log messages.
     * 
     * @return  the event to be associated
     */
    public String getEventName( );
    
    /**
     * Creates the start message for the event.
     * 
     * @return start event message
     */
    public String getStartEventMessage();
    
    /**
     * Creates the end message for the event.
     * 
     * @return end event message
     */
    public String getEndEventMessage();
    
    
    /**
     * Reset the internal log message buffer associated with the event
     * 
     */
    public void reset();

    /**
     * Add to the log message.
     * 
     * @param key 
     * @param value
     * 
     * @return Self-reference, so calls can be chained
     */
    public Event add( String key, String value );
    
    /**
     * Creates a log message with the contents of the internal log buffer.
     */
    public String createLogMessage();
    
    
    /**
     * Creates a log message with the contents of the internal log buffer.
     * It then resets the buffer before returning the log message
     * 
     * @return 
     */
    public String createLogMessageAndReset();
    
}
