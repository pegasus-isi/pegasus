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

package edu.isi.pegasus.planner.selector.site;


import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import org.griphyn.cPlanner.classes.ADag;
import org.griphyn.cPlanner.classes.PegasusBag;

import edu.isi.pegasus.common.logging.LogManager;

import org.griphyn.cPlanner.common.PegasusProperties;

import org.griphyn.cPlanner.partitioner.graph.Adapter;

import edu.isi.pegasus.planner.selector.SiteSelector;

import edu.isi.pegasus.planner.catalog.transformation.Mapper;

import java.util.List;


/**
 * The Abstract Site selector.
 *
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @author Gaurang Mehta
 *
 *
 * @version $Revision$
 */
public abstract class Abstract implements SiteSelector {

    /**
     * The properties passed to Pegasus at runtime.
     */
    protected PegasusProperties mProps;

    /**
     * The handle to the logger.
     */
    protected LogManager mLogger;

    /**
     * The handle to the site catalog.
     */
//    protected PoolInfoProvider mSCHandle;
    protected SiteStore mSiteStore;

    /**
     * The handle to the TCMapper object.
     */
    protected Mapper mTCMapper;

    /**
     * The bag of Pegasus objects.
     */
    protected PegasusBag mBag;

    /**
     * Initializes the site selector.
     *
     * @param bag   the bag of objects that is useful for initialization.
     *
     */
    public void initialize( PegasusBag bag ){
        mBag   =  bag;
        mProps =  ( PegasusProperties )bag.get( PegasusBag.PEGASUS_PROPERTIES );
        mLogger   = ( LogManager )bag.get( PegasusBag.PEGASUS_LOGMANAGER );
        mSiteStore = bag.getHandleToSiteStore();
        mTCMapper = ( Mapper )bag.get( PegasusBag.TRANSFORMATION_MAPPER );
    }



    /**
     * Maps the jobs in the workflow to the various grid sites.
     * The jobs are mapped by setting the site handle for the jobs.
     *
     * @param workflow   the workflow.
     *
     * @param sites     the list of <code>String</code> objects representing the
     *                  execution sites that can be used.
     */
    public void mapWorkflow( ADag workflow, List sites ){
         mapWorkflow( Adapter.convert( workflow ), sites );
    }

}
