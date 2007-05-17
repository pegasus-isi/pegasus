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

package org.griphyn.cPlanner.visualize;

import java.util.Date;
import java.util.List;
import java.util.Iterator;
import java.util.ArrayList;

/**
 * A data class that associates at most three measurements with the job
 * corresponding to the GRIDSTART_PREJOB, GRIDSTART_MAINJOB and GRIDSTART_POSTJOB.
 *
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */

public class JobMeasurements {

    /**
     * The PREJOB data index.
     */
    public static final int GRIDSTART_PREJOB_EVENT_TYPE = 0;

    /**
     * The MAINJOB data index.
     */
    public static final int GRIDSTART_MAINJOB_EVENT_TYPE = 1;

    /**
     * The POSTJOB data index.
     */
    public static final int GRIDSTART_POSTJOB_EVENT_TYPE = 2;

    /**
     * The name of the job.
     */
    private String mName;

    /**
     * The list of Measurement objects.
     */
    private List mMeasurementList;

    /**
     * The corresponding list of Date objects signifying the times at which
     * an event happened.
     */
    private List mTimeList;

    /**
     * The default constructor.
     */
    public JobMeasurements(){
        mMeasurementList = new ArrayList( 3 );
        for( int i = 0; i < 3; i++ ){ mMeasurementList.add( null ); }

        mTimeList = new ArrayList( 3 );
        for( int i = 0; i < 3; i++ ){ mTimeList.add( null ); }
    }

    /**
     * The overloaded constructor.
     *
     * @param name  the name of the job
     */
    public JobMeasurements( String name ){
        this();
        mName = name;
    }

    /**
     * Adds a measurement for a particular event type.
     *
     * @param measurement the measurement to be associated
     * @param type        the event type
     */
    public void setMeasurement( Measurement measurement, int type ){
        if ( !typeInRange( type ) ){
            throw new NumberFormatException( "Event type specified is not in range " + type );
        }

        mMeasurementList.set( type, measurement );
        //mTimeList.set( type, measurement.getTime() );
    }

    /**
     * Adds a time for a particular event type.
     *
     * @param time  the time
     * @param type  the event type
     */
    public void setTime( Date time, int type ){
        if ( !typeInRange( type ) ){
            throw new NumberFormatException( "Event type specified is not in range " + type );
        }

        mTimeList.set( type, time );
    }


    /**
     * Returns the measurement corresponding to the event type.
     *
     * @param type event type.
     *
     * @return <code>Measurement</code> object if data exists else null
     */
    public Measurement getMeasurement( int type ){
        if ( !typeInRange( type ) ){
            throw new NumberFormatException( "Event type specified is not in range " + type );
        }
        Object obj = mMeasurementList.get( type );
        return ( obj == null ) ? null : ( Measurement )obj;

    }

    /**
     * Returns the readings iterator. Values can be null.
     *
     * @return iterator to measurements.
     */
    public Iterator measurementsIterator(){
        return mMeasurementList.iterator();
    }

    /**
     * Returns a boolean indicating whether the event type is in range of not.
     *
     * @param type the type value
     */
    public boolean typeInRange( int type ){
        return ( type >= GRIDSTART_PREJOB_EVENT_TYPE &&
                 type <= GRIDSTART_POSTJOB_EVENT_TYPE );
    }

    /**
     * Returns a textual description of the object.
     *
     * @return description
     */
    public String toString(){
        StringBuffer sb = new StringBuffer();
        sb.append( mName ).append( " ").append( mMeasurementList );
        return sb.toString();
    }
}
