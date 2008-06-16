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

package org.griphyn.cPlanner.visualize.spaceusage;

import java.util.Date;

/**
 * A data class that stores the space usage.
 *
 * @author Karan Vahi
 * @version $Revision$
 */

public class Space implements Cloneable{

    /**
     * Holds the timestamp when usage was taken.
     */
    private Date mDate;

    /**
     * The size.
     */
    private float mSize;

    /**
     * The units in which the size is stored.
     * M for megabyte, K for Kilobytes, T for terabytes.
     * The default unit is K
     */
    private char mUnit;

    /**
     * index to do conversions between units.
     */
    private int mIndex;

    /**
     * Indicates whether the space denoted is the amount of space
     * cleaned up.
     */
    private boolean mCleanupSpace;

    /**
     * The jobname for which the reading was taken.
     */
    private String mAssociatedJob;

    /**
     * The overloaded constructor.
     *
     * @param d the date.
     */
    public Space( Date d ){
        mDate = d;
        mUnit = 'K';
        mIndex = getIndex( mUnit );
        mCleanupSpace = false;
    }


    /**
     * The overloaded constructor.
     *
     * @param d Date
     * @param size the size with the last character denoting the unit.
     */
    public Space( Date d, String size ) {
        mDate = d;
        mUnit = 'K';
        mIndex = getIndex( mUnit );
        setSize( size );
        mCleanupSpace = false;
    }

    /**
     * Sets boolean indicating that space denoted is the amount cleaned.
     *
     * @param cleanup  boolean indicating that value is cleaned up.
     */
    public void setCleanupFlag( boolean cleanup ){
        mCleanupSpace = cleanup;
    }

    /**
     * Sets the associated job for which the reading was taken.
     *
     * @param job  the associated job.
     */
    public void setAssociatedJob( String job ){
        mAssociatedJob = job;
    }

    /**
     * Returns the associated job.
     *
     * @return the associated job.
     */
    public String getAssociatedJob(){
        return mAssociatedJob;
    }


    /**
     * Returns boolean indicating whether the space denoted is the amount cleaned
     * or not.
     *
     * @return  boolean indicating that value is cleaned up.
     */
    public boolean getCleanupFlag(  ){
        return mCleanupSpace;
    }

    /**
     * Sets the size.
     *
     * @param size  the size optionally with the units.
     * @param unit  the unit of the size
     */
    public void setSize( float size, char unit ) {
        mSize  = size;
        mUnit  = unit;
        mIndex = getIndex( unit );
    }

    /**
     * Sets the size.
     *
     * @param size  the size optionally with the units.
     */
    public void setSize( String size ) {
        size = size.trim();
        char c = size.charAt( size.length() - 1);

        if ( Character.isLetter( c ) ){
            if ( validUnit(c)){
                mUnit = c;
                mIndex = this.getIndex( c );
                mSize = Float.parseFloat( size.substring( 0, size.length() - 1));
            }
            else {
                throw new RuntimeException( "Invald unit " + c );
            }
        }
        else{
            mSize = Float.parseFloat(size);
            mUnit = 'K';
        }
    }

    /**
     * Returns the time at which Space was record.
     *
     * @return Date
     */
    public Date getDate( ){
        return mDate;
    }

    /**
     * Returns the size in the units associated with it.
     *
     * @return size in float
     */
    public float getSize(){
        return mSize;
    }

    /**
     * Returns the size in particular unit.
     *
     * @param u  the unit.
     *
     * @return size in float
     */
    public float getSize( char u ){
        int index = getIndex( u );
        //System.out.println( "difference is " + (mIndex - index) );
        //System.out.println( "multiplying factor is " + this.getMultiplyingFactor( 1024, mIndex - index));
        //return mSize * (float) Math.pow( 1024, mIndex - index );
        return mSize * this.getMultiplyingFactor( 1024, mIndex -index );
    }


    /**
     * Returns the units associated with the size.
     *
     * @return the unit
     */
    public char getUnits(){
        return mUnit;
    }

    /**
     * Returns if a character is a valid unit or not
     *
     * @param c  the unit
     *
     * @return boolean
     */
    public boolean validUnit( char c ){
        return ( c == 'K' || c == 'M' || c == 'G');
    }

    /**
     * Returns a textual description of the content
     *
     * @return String.
     */
    public String toString(){
        StringBuffer sb = new StringBuffer();
        sb.append( mDate ).append( " " ).append( mSize ).append( mUnit );
        sb.append( " " ).append( getAssociatedJob() );
        if( mCleanupSpace ){ sb.append( " cleaned up space" ); }
        return sb.toString();
    }

    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone(){
        Space s;
        try{
            s = ( Space) super.clone();
        }
        catch( CloneNotSupportedException e ){
            //somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException("Clone not implemented in the base class of " + this.getClass().getName(),
                                       e );
        }
        s.mAssociatedJob = this.mAssociatedJob;
        s.mCleanupSpace  = this.mCleanupSpace;
        s.mDate          = (Date)this.mDate.clone();
        s.mIndex         = this.mIndex;
        s.mSize          = this.mSize;
        s.mUnit          = this.mUnit;

        return s;
    }

    /**
     * Returns the index for the associated unit.
     *
     * @return the index.
     */
    private int getIndex(){
        return mIndex;
    }

    /**
     * Returns the index for a unit.
     *
     * @param u  the unit
     * @return the index.
     */
    private int getIndex( char u ){
        int index = -1;
        switch ( u ){
            case 'K':
                index = 1;
                break;

            case 'M':
                index = 2;
                break;

            case 'G':
                index = 3;
                break;

            default:
                throw new RuntimeException( "Invalid unit scheme" + u );
        }
        return index;
    }


    /**
     * Returns multiplying factor for conversion.
     * Simulates ^ operator.
     *
     * @param base
     * @param power  the power to raise the base to.
     *
     * @return multiplying value
     */
    private float getMultiplyingFactor( int base, int power){
        float result = 1;

        if ( power >= 0 ){
            for( int i = 0; i < power; i++){
                result *= base;
            }
        }
        else{
            for( int i = 0; i < -power; i++){
                result /= base;
            }
        }
        return result;
    }


}
