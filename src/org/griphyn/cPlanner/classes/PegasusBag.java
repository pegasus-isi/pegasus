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

package org.griphyn.cPlanner.classes;

import org.griphyn.cPlanner.partitioner.graph.Bag;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.common.LogManager;

import org.griphyn.cPlanner.poolinfo.PoolInfoProvider;

import org.griphyn.common.catalog.TransformationCatalog;
import org.griphyn.common.catalog.ReplicaCatalog;

import org.griphyn.common.catalog.transformation.Mapper;

/**
 * A bag of objects that needs to be passed to various refiners.
 * It contains handles to the various catalogs, the properties and the
 * planner options.
 *
 * @author Karan Vahi
 * @version $Revision$
 */
public class PegasusBag
    implements Bag {

    /**
     * Array storing the names of the attributes that are stored with the
     * site.
     */
    public static final String PEGASUS_INFO[] = {
        "pegasus-properties", "planner-options", "replica-catalog", "site-catalog",
        "transformation-catalog", "pegasus-logger"
    };


    /**
     * The constant to be passed to the accessor functions to get or set the
     * PegasusProperties.
     */
    public static final Integer PEGASUS_PROPERTIES = new Integer( 0 );

    /**
     * The constant to be passed to the accessor functions to get or set the
     * options passed to the planner.
     */
    public static final Integer PLANNER_OPTIONS = new Integer( 1 );

    /**
     * The constant to be passed to the accessor functions to get or set the
     * handle to the replica catalog
     */
    public static final Integer REPLICA_CATALOG = new Integer( 2 );

    /**
     * The constant to be passed to the accessor functions to get or set the
     * handle to the site catalog.
     */
    public static final Integer SITE_CATALOG = new Integer( 3 );

    /**
     * The constant to be passed to the accessor functions to get or set the
     * handle to the transformation catalog.
     */
    public static final Integer TRANSFORMATION_CATALOG = new Integer( 4 );

    /**
     * The constant to be passed to the accessor functions to get or set the
     * handle to the Transformation Mapper.
     */
    public static final Integer TRANSFORMATION_MAPPER = new Integer( 5 );

    /**
     * The constant to be passed to the accessor functions to get or set the
     * handle to the Logging manager
     */
    public static final Integer PEGASUS_LOGMANAGER = new Integer( 6 );


    /**
     * The handle to the <code>PegasusProperties</code>.
     */
    private PegasusProperties mProps;

    /**
     * The options passed to the planner.
     */
    private PlannerOptions mPOptions;

    /**
     * The handle to the replica catalog.
     */
    private ReplicaCatalog mRCHandle;

    /**
     * The handle to the site catalog.
     */
    private PoolInfoProvider mSCHandle;

    /**
     * The handle to the transformation catalog.
     */
    private TransformationCatalog mTCHandle;

    /**
     * The handle to the Transformation Mapper.
     */
    private Mapper mTCMapper;

    /**
     * The handle to the LogManager.
     */
    private LogManager mLogger;

    /**
     * The default constructor.
     */
    public PegasusBag() {
    }

    /**
     * Adds an object to the underlying bag corresponding to a particular key.
     *
     * @param key the key with which the value has to be associated.
     * @param value the value to be associated with the key.
     *
     * @return boolean indicating if insertion was successful.
     *
     */
    public boolean add( Object key, Object value ) {
        //to denote if object is of valid type or not.
        boolean valid = true;
        int k = getIntValue( key );

        switch ( k ) {

            case 0: //PEGASUS_PROPERTIES
                if ( value != null && value instanceof PegasusProperties)
                    mProps = (PegasusProperties) value;
                else
                    valid = false;
                break;

            case 1: //PLANNER_OPTIONS
                if ( value != null && value instanceof PlannerOptions )
                    mPOptions = ( PlannerOptions ) value;
                else
                    valid = false;
                break;

            case 2: //REPLICA_CATALOG:
                if ( value != null && value instanceof ReplicaCatalog )
                    mRCHandle = ( ReplicaCatalog ) value;
                else
                    valid = false;
                break;

            case 3: //SITE_CATALOG:
                if ( value != null && value instanceof PoolInfoProvider )
                    mSCHandle = ( PoolInfoProvider ) value;
                else
                    valid = false;
                break;

            case 4: //TRANSFORMATION_CATALOG:
                if ( value != null && value instanceof TransformationCatalog )
                    mTCHandle = ( TransformationCatalog ) value;
                else
                    valid = false;
                break;

            case 5: //TRANSFORMATION_MAPPER
                if ( value != null && value instanceof Mapper )
                    mTCMapper = ( Mapper ) value;
                else
                    valid = false;
                break;

            case 6: //PEGASUS_LOGGER
                if ( value != null && value instanceof LogManager )
                    mLogger = ( LogManager ) value;
                else
                    valid = false;
                break;

            default:
                throw new RuntimeException(
                      " Wrong Pegasus Bag key. Please use one of the predefined Integer key types");
        }

        //if object is not null , and valid == false
        //throw exception
        if( !valid && value != null ){
            throw new RuntimeException( "Invalid object passed for key " +
                                        PEGASUS_INFO[ k ]);
        }

        return valid;
    }

    /**
     * Returns true if the namespace contains a mapping for the specified key.
     *
     * @param key The key that you want to search for in the bag.
     *
     * @return boolean
     */
    public boolean containsKey(Object key) {

        int k = -1;
        try{
            k = ( (Integer) key).intValue();
        }
        catch( Exception e ){}

        return ( k >= this.PEGASUS_PROPERTIES.intValue() && k <= this.TRANSFORMATION_CATALOG.intValue() );
    }

    /**
     * Returns an objects corresponding to the key passed.
     *
     * @param key the key corresponding to which the objects need to be
     *            returned.
     *
     * @return the object that is found corresponding to the key or null.
     */
    public Object get( Object key ) {
        int k = getIntValue( key );

        switch( k ){
            case 0:
                return this.mProps;

            case 1:
                return this.mPOptions;

            case 2:
                return this.mRCHandle;

            case 3:
                return this.mSCHandle;

            case 4:
                return this.mTCHandle;

            case 5: //TRANSFORMATION_MAPPER
                return this.mTCMapper;

            case 6: //PEGASUS_LOGMANAGER
                return this.mLogger;

            default:
                throw new RuntimeException(
                    " Wrong Pegasus Bag key. Please use one of the predefined Integer key types");
        }
    }


    /**
     * A convenice method to get PlannerOptions
     *
     * @return  the handle to options passed to the planner.
     */
    public PlannerOptions getPlannerOptions(){
        return ( PlannerOptions )get( PegasusBag.PLANNER_OPTIONS );
    }


    /**
     * A convenice method to get PegasusProperties
     *
     * @return  the handle to the properties.
     */
    public PegasusProperties getPegasusProperties(){
        return ( PegasusProperties )get( PegasusBag.PEGASUS_PROPERTIES );
    }

    /**
     * A convenice method to get Logger/
     *
     * @return  the handle to the logger.
     */
    public LogManager getLogger(){
        return ( LogManager )get( PegasusBag.PEGASUS_LOGMANAGER );
    }

    /**
     * A convenice method to get the handle to the site catalog.
     *
     * @return  the handle to site catalog
     */
    public PoolInfoProvider getHandleToSiteCatalog(){
        return ( PoolInfoProvider )get( PegasusBag.SITE_CATALOG );
    }

    /**
     * A convenice method to get the handle to the transformation catalog.
     *
     * @return  the handle to transformation catalog
     */
    public TransformationCatalog getHandleToTransformationCatalog(){
        return ( TransformationCatalog )get( PegasusBag.TRANSFORMATION_CATALOG );
    }


    /**
     * A convenice method to get the handle to the transformation mapper.
     *
     * @return  the handle to transformation catalog
     */
    public Mapper getHandleToTransformationMapper(){
        return ( Mapper )get( PegasusBag.TRANSFORMATION_MAPPER );
    }



    /**
     * A convenience method to get the intValue for the object passed.
     *
     * @param key   the key to be converted
     *
     * @return the int value if object an integer, else -1
     */
    private int getIntValue( Object key ){

        int k = -1;
        try{
            k = ( (Integer) key).intValue();
        }
        catch( Exception e ){}

        return k;

    }
}
