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

package edu.isi.pegasus.planner.parser.dax;

import edu.isi.pegasus.planner.parser.StackBasedXMLParser;

import edu.isi.pegasus.common.logging.LogManagerFactory;

import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.common.util.CondorVersion;
import edu.isi.pegasus.common.util.Separator;
import edu.isi.pegasus.common.util.Version;

import edu.isi.pegasus.planner.catalog.classes.SysInfo;

import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.impl.Abstract;

import edu.isi.pegasus.planner.classes.CompoundTransformation;
import edu.isi.pegasus.planner.classes.DAGJob;
import edu.isi.pegasus.planner.classes.DAXJob;
import edu.isi.pegasus.planner.classes.Job;
import edu.isi.pegasus.planner.classes.PCRelation;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.PegasusFile;
import edu.isi.pegasus.planner.classes.PegasusFile.LINKAGE;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.classes.ReplicaLocation;

import edu.isi.pegasus.planner.code.GridStartFactory;


import edu.isi.pegasus.planner.dax.Invoke;
import edu.isi.pegasus.planner.dax.Invoke.WHEN;
import edu.isi.pegasus.planner.dax.MetaData;
import edu.isi.pegasus.planner.dax.PFN;

import edu.isi.pegasus.planner.namespace.Hints;
import edu.isi.pegasus.planner.namespace.Pegasus;


import java.io.File;
import java.io.IOException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.xml.sax.SAXException;

/**
 * This class uses the Xerces SAX2 parser to validate and parse an XML
 * document conforming to the DAX Schema 3.2
 * 
 * @author Karan Vahi vahi@isi.edu
 * @version $Revision$
 */
public class DAXParser3 extends StackBasedXMLParser implements DAXParser {

   
    /**
     * The "not-so-official" location URL of the Site Catalog Schema.
     */
    public static final String SCHEMA_LOCATION =
                                        "http://pegasus.isi.edu/schema/dax-3.2.xsd";

    /**
     * uri namespace
     */
    public static final String SCHEMA_NAMESPACE =
                                        "http://pegasus.isi.edu/schema/DAX";

    
    /**
     * Constant denoting an undefined site
     */
    public static final String UNDEFINED_SITE = "undefined";

    /*
     * Predefined Constant for condor version 7.1.0
     */
    public static final long DAX_VERSION_3_2_0 = CondorVersion.numericValue( "3.2.0" );
    
    /**
     * Constant denoting default metadata type
     */
    private String DEFAULT_METADATA_TYPE = "String";

    
    /**
     * List of parents for a child node in the graph
     */
    protected List<PCRelation> mParents;


    /**
     * Handle to the callback
     */
    protected Callback mCallback;

    /**
     * A job prefix specifed at command line.
     */
    protected String mJobPrefix;
    
   
    
    /**
     * The overloaded constructor.
     *
     * @param properties the <code>PegasusProperties</code> to be used.
     */
    public DAXParser3( PegasusBag bag  ) {
        super( bag );
        mJobPrefix = ( bag.getPlannerOptions() == null ) ?
                       null:
                       bag.getPlannerOptions().getJobnamePrefix();

    }

   
    /**
     * Set the DAXCallback for the parser to call out to.
     *
     * @param c  the callback
     */
    public void setDAXCallback( Callback c ){
        this.mCallback = c;
    }

    /**
     * Retuns the DAXCallback for the parser
     *
     * @return  the callback
     */
    public Callback getDAXCallback(  ){
        return this.mCallback;
    }

    /**
     * The main method that starts the parsing.
     * 
     * @param file   the XML file to be parsed.
     */
    public void startParser( String file ) {
        try {
            this.testForFile( file );
            mParser.parse( file );
            
            //sanity check
            if ( mDepth != 0 ){
                throw new RuntimeException( "Invalid stack depth at end of parsing " + mDepth );
            }
        } catch ( IOException ioe ) {
            mLogger.log( "IO Error :" + ioe.getMessage(),
                        LogManager.ERROR_MESSAGE_LEVEL );
        } catch ( SAXException se ) {

            if ( mLocator != null ) {
                mLogger.log( "Error in " + mLocator.getSystemId() +
                    " at line " + mLocator.getLineNumber() +
                    " at column " + mLocator.getColumnNumber() + " :" +
                    se.getMessage() , LogManager.ERROR_MESSAGE_LEVEL);
            }
        }
    }

    /**
     * Returns the XML schema namespace that a document being parsed conforms
     * to.
     *
     * @return the schema namespace
     */
    public  String getSchemaNamespace( ){
        return DAXParser3.SCHEMA_NAMESPACE;
    }

    /**
     * Returns the local path to the XML schema against which to validate.
     *
     * @return path to the schema
     */
    public String getSchemaLocation() {
        // treat URI as File, yes, I know - I need the basename
        File uri = new File( DAXParser3.SCHEMA_LOCATION );
        // create a pointer to the default local position
        File dax = new File( this.mProps.getSysConfDir(),  uri.getName() );

        return this.mProps.getDAXSchemaLocation( dax.getAbsolutePath() );

    }



    /**
     * Composes the  <code>SiteData</code> object corresponding to the element
     * name in the XML document.
     *
     * @param element the element name encountered while parsing.
     * @param names   is a list of attribute names, as strings.
     * @param values  is a list of attribute values, to match the key list.
     *
     * @return the relevant SiteData object, else null if unable to construct.
     *
     * @exception IllegalArgumentException if the element name is too short.
     */
    public  Object createObject( String element, List names, List values ){

         if ( element == null || element.length() < 1 ){
            throw new IllegalArgumentException("illegal element length");
        }


        switch ( element.charAt(0) ) {
            // a adag argument
            case 'a':
                if ( element.equals( "adag" ) ) {
                    //for now the adag element is just a map of
                    //key value pair
                    Map<String,String> m = new HashMap();
                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );
                        m.put( name, value );
                    }

                    sanityCheckOnVersion( m.get( "version" ) );
                    
                    //put the call to the callback
                    this.mCallback.cbDocument( m );
                    return m;
                }//end of element adag
                else if( element.equals( "argument" ) ){
                    return new Arguments();
                }
                return null;

            //c child compound
            case 'c':
                if( element.equals( "child") ){
                    this.mParents = new LinkedList<PCRelation>();
                    PCRelation pc = new PCRelation();
                    String child = null;
                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "ref" ) ) {
                            child = value;
                        }
                    }
                    if( child == null ){
                        this.complain( element, "child", child );
                        return null;
                    }
                    pc.setChild( child );
                    return pc;
                }
                else if ( element.equals( "compound") ){

                }
                return null;


            //d dag dax
            case 'd':
                if( element.equals( "dag" ) || element.equals( "dax" ) ){
                    Job j = new Job( );
                    //all jobs in the DAX are of type compute
                    j.setUniverse( GridGateway.JOB_TYPE.compute.toString() );
                    j.setJobType( Job.COMPUTE_JOB );
                    
                    String file = null;
                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "namespace" ) ) {
                            j.setTXNamespace( value );
                        }
                        else if( name.equals( "name" ) ){
                            j.setTXName( value );
                        }
                        else if( name.equals( "version" ) ){
                            j.setTXVersion( value );
                        }
                        else if( name.equals( "id"  ) ){
                            j.setLogicalID( value );
                        }
                        else if( name.equals( "file" ) ){
                            file = value;
                        }
                        else if( name.equals( "node-label"  ) ){
                            this.attributeNotSupported( element, name, value );
                        }
                        else {
                	    this.complain( element, name, value );
                        }
                    }


                    if( file == null ){
                        this.complain( element, "file", file );
                        return null;
                    }
                    PegasusFile pf = new PegasusFile( file );
                    pf.setLinkage( LINKAGE.INPUT );
                    
                    if( element.equals( "dag" ) ){
                        DAGJob dagJob = new DAGJob( j );
                        dagJob.setDAGLFN( file );
                        dagJob.addInputFile( pf );
                        
                        //the job should always execute on local site
                        //for time being
                        dagJob.hints.construct(Hints.EXECUTION_POOL_KEY, "local");

                        //also set the executable to be used
                        dagJob.hints.construct(Hints.PFN_HINT_KEY, "/opt/condor/bin/condor-dagman");



                        //add default name and namespace information
                        dagJob.setTransformation("condor",
                                "dagman",
                                null);


                        dagJob.setDerivation("condor",
                                "dagman",
                                null);

                        dagJob.level = -1;

                        //dagman jobs are always launched without a gridstart
                        dagJob.vdsNS.construct(Pegasus.GRIDSTART_KEY,
                                GridStartFactory.GRIDSTART_SHORT_NAMES[GridStartFactory.NO_GRIDSTART_INDEX]);


                        //set the internal primary id for job
                        //dagJob.setName( constructJobID( dagJob ) );
                        dagJob.setName( dagJob.generateName( this.mJobPrefix) );
                        return dagJob;
                    }
                    else if (element.equals( "dax" ) ){
                        DAXJob daxJob = new DAXJob( j );

                        //the job should be tagged type pegasus
                        daxJob.setTypeRecursive();

                        //the job should always execute on local site
                        //for time being
                        daxJob.hints.construct( Hints.EXECUTION_POOL_KEY, "local" );

                        //also set a fake executable to be used
                        daxJob.hints.construct( Hints.PFN_HINT_KEY, "/tmp/pegasus-plan" );

                        //retrieve the extra attribute about the DAX
                        daxJob.setDAXLFN( file );
                        daxJob.addInputFile( pf );

                        //add default name and namespace information
                        daxJob.setTransformation( "pegasus",
                                                  "pegasus-plan",
                                                  Version.instance().toString() );


                        daxJob.setDerivation( "pegasus",
                                              "pegasus-plan",
                                               Version.instance().toString() );

                        daxJob.level       = -1;

                        //set the internal primary id for job
                        //daxJob.setName( constructJobID( daxJob ) );
                        daxJob.setName( daxJob.generateName( this.mJobPrefix) );
                        return daxJob;
                    }

                }//end of element job
                return null;//end of j

                
            //e executable
            case 'e':
                if( element.equals( "executable" ) ){
                    TransformationCatalogEntry tce = new TransformationCatalogEntry();
                    SysInfo sysinfo = new SysInfo();

                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "namespace" ) ) {
                            tce.setLogicalNamespace( value );
                        }
                        else if( name.equals( "name" ) ){
                            tce.setLogicalName( value );
                        }
                        else if( name.equals( "version" ) ){
                            tce.setLogicalVersion( value );
                        }
                        else if( name.equals( "arch" ) ){
                            sysinfo.setArchitecture( SysInfo.Architecture.valueOf( value.toLowerCase() ) );
                        }
                        else if( name.equals( "os" ) ){
                            sysinfo.setOS( SysInfo.OS.valueOf( value.toUpperCase() ) );
                        }
                        else if( name.equals( "osrelease" ) ){
                            sysinfo.setOSVersion( value );
                        }
                        else if( name.equals( "osversion" ) ){
                            sysinfo.setOSVersion( value );
                        }
                        else if( name.equals( "glibc" ) ){
                            sysinfo.setGlibc( value );
                        }
                        else if( name.equals( "installed" ) ){
                            Boolean installed = Boolean.parseBoolean( value );
                            //ignore dont need to do anything
                            tce.setType( installed ? TCType.INSTALLED : TCType.STAGEABLE  );
                        }
                    }
                    tce.setSysInfo(sysinfo);
                    return tce;
                }//end of element executable

                return null; //end of e

            //f file
            case 'f':
                if( element.equals( "file" ) ){
                    //create a FileTransfer Object or shd it be ReplicaLocations?
                    //FileTransfer ft = new FileTransfer();
                    ReplicaLocation rl = new ReplicaLocation();

                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "name" ) ) {
                            //ft.setLFN( value );
                            rl.setLFN( value );
                        }
                        else if( name.equals( "link" ) ){
                            //ignore dont need to do anything
                        }
                        else if( name.equals( "optional"  ) ){
                            Boolean optional = Boolean.parseBoolean( value );
                            if( optional ){
                               //replica location object does not handle
                                //optional attribute right now.
                               // ft.setFileOptional();
                            }
                        }
                        else {
                	    this.complain( element, name, value );
                        }
                    }

                    return rl;
                }//end of element file

                return null; //end of f

            //i invoke
            case 'i':
                if( element.equals( "invoke" ) ){

                    String when = null;
                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "when" ) ) {
                            when = value;
                 	    this.log( element, name, value );
                        }
                        else {
                	    this.complain( element, name, value );
                        }
                    }
                    if( when == null ){
                        this.complain( element, "when", when );
                        return null;
                    }
                    return new Invoke( WHEN.valueOf( when ) );

                }//end of element invoke
                return null;

            //j job
            case 'j':
                if( element.equals( "job" ) ){
                    Job j = new Job( );
                    //all jobs in the DAX are of type compute
                    j.setUniverse( GridGateway.JOB_TYPE.compute.toString() ); 
                    j.setJobType( Job.COMPUTE_JOB );
                    
                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "namespace" ) ) {
                            j.setTXNamespace( value );
                        }
                        else if( name.equals( "name" ) ){
                            j.setTXName( value );
                        }
                        else if( name.equals( "version" ) ){
                            j.setTXVersion( value );
                        }
                        else if( name.equals( "id"  ) ){
                            j.setLogicalID( value );
                        }
                        else if( name.equals( "node-label"  ) ){
                            this.attributeNotSupported( element, name, value );
                        }
                        else {
                	    this.complain( element, name, value );
                        }
                    }

                    //set the internal primary id for job
                    j.setName( constructJobID( j ) );
                    return j;
                }//end of element job
                return null;//end of j

            //m metadata
            case 'm':
                if( element.equals( "metadata" ) ){

                    String key = null;
                    String type = DEFAULT_METADATA_TYPE;
                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "key" ) ) {
                            key = value;
                 	    this.log( element, name, value );
                        }
                        else if ( name.equals( "type" ) ) {
                            type = value;
                            this.log( element, name, value );
                        }
                        else {
                	    this.complain( element, name, value );
                        }
                    }
                    if( key == null ){
                        this.complain( element, "key", key );
                    }
                    MetaData md = new MetaData( key, type );
                    return md;

                }//end of element metadata

                return null;//end of case m

            //p parent profile pfn
            case 'p':
                if( element.equals( "parent" ) ){
                    String parent = null;

                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "ref" ) ) {
                            parent = value;
                        }
                        else if( name.equals( "edge-label" ) ){
                            this.attributeNotSupported( "parent", "edge-label", value);
                        }
                        else {
                	    this.complain( element, name, value );
                        }

                    }
                    if( parent == null ){
                        this.complain( element, "parent", parent );
                        return null;
                    }
                    return parent;

                }
                else if( element.equals( "profile" ) ){
                    Profile p = new Profile();
                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "namespace" ) ) {
                            p.setProfileNamespace( value.toLowerCase() );
                 	    this.log( element, name, value );
                        }
                        else if ( name.equals( "key" ) ) {
                            p.setProfileKey( value );
                 	    this.log( element, name, value );
                        }
                        else {
                	    this.complain( element, name, value );
                        }
                    }
                    return p;
                }//end of element profile
                else if( element.equals( "pfn" ) ){

                    String url = null;
                    String site = UNDEFINED_SITE;
                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "url" ) ) {
                            url = value;
                 	    this.log( element, name, value );
                        }
                        else if ( name.equals( "site" ) ) {
                            site = value;
                            this.log( element, name, value );
                        }
                        else {
                	    this.complain( element, name, value );
                        }
                    }
                    if( url == null ){
                        this.complain( element, "url", url );
                        return null;
                    }
                    PFN pfn = new PFN( url, site );
                    return pfn;
                }//end of element pfn              
                return null;//end of case p


            //s stdin stdout stderr
            case 's':
                if( element.equals( "stdin" ) ||
                    element.equals( "stdout" ) ||
                    element.equals( "stderr") ){
                    //we use DAX API File object for this
                    String fileName = null;
                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "name" ) ) {
                            fileName = value;
                 	    this.log( element, name, value );
                        }
                        else if ( name.equals( "link" ) ) {
                            //we ignore as linkage is fixed for stdout|stderr|stdin
                            this.log( element, name, value );
                        }
                        else {
                	    this.complain( element, name, value );
                        }
                    }
                    if( fileName == null ){
                        this.complain( element, "name", fileName );
                        return null;
                    }
                    return new edu.isi.pegasus.planner.dax.File( fileName );
                }//end of stdin|stdout|stderr
                return null;//end of case s

            //t transformation
            case 't':
                if( element.equals( "transformation" ) ){
                    String namespace = null,lname = null, version = null;
                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        if ( name.equals( "namespace" ) ) {
                            namespace = value;
                        }
                        else if( name.equals( "name" ) ){
                            lname = value;
                        }
                        else if( name.equals( "version" ) ){
                            version = value;
                        }
                    }
                    return new CompoundTransformation( namespace, lname, version );
                }
                return null;

            //u uses
            case 'u':
                if( element.equals( "uses" ) ){
                    PegasusFile pf = new PegasusFile( );
                    String fName = null;
                    String fNamespace = null;
                    String fVersion = null;
                    for ( int i=0; i < names.size(); ++i ) {
                        String name = (String) names.get( i );
                        String value = (String) values.get( i );

                        /*
                         * Name  	Type  	Use  	Default  	Fixed
                            name  	xs:string  	required
                            link  	LinkageType  	optional
                            optional  	xs:boolean  	optional  	false
                            register  	xs:boolean  	optional  	true
                            transfer  	TransferType  	optional  	true
                            namespace  	xs:string  	optional
                            version  	VersionPattern  	optional
                            exectuable  	xs:boolean  	optional  	false
                         */
                        if ( name.equals( "name" ) ) {
                            pf.setLFN( value );
                            fName = value;
                 	    this.log( element, name, value );
                        }
                        else if ( name.equals( "link" ) ) {
                            pf.setLinkage( PegasusFile.LINKAGE.valueOf( value.toUpperCase() ) );
                            this.log( element, name, value );
                        }
                        else if ( name.equals( "optional" ) ) {
                            Boolean bValue = Boolean.parseBoolean( value );
                            if( bValue ){
                               pf.setFileOptional();
                            }
                 	    this.log( element, name, value );
                        }
                        else if( name.equals( "register") ){
                            Boolean bValue = Boolean.parseBoolean( value );
                            if( bValue ){
                                pf.setRegisterFlag( bValue );
                            }else{
                                pf.setRegisterFlag( !bValue );
                            }
                        }
                        else if ( name.equals( "transfer" ) ) {
                            pf.setTransferFlag( value );
                            this.log( element, name, value );
                        }
                        else if ( name.equals( "namespace" ) ) {
                            fNamespace = value;
                            this.log( element, name, value );
                        }
                        else if ( name.equals( "version" ) ) {
                            fVersion = value;
                            this.log( element, name, value );
                        }
                        else if ( name.equals( "executable" ) ) {
                            Boolean bValue = Boolean.parseBoolean( value );
                            if( bValue ){
                               pf.setType( PegasusFile.EXECUTABLE_FILE );
                            }
                 	    this.log( element, name, value );
                        }
                        else if ( name.equals( "size" ) ) {
                            pf.setSize( value );
                            this.log( element, name, value );
                        }
                        else {
                	    this.complain( element, name, value );
                        }

                    }

                    //if executable then update lfn to combo of namespace,name,version
                    if( pf.getType() == PegasusFile.EXECUTABLE_FILE ){
                        pf.setLFN( Separator.combine(fNamespace, fName, fVersion) );
                    }
                    return pf;

                }//end of uses
                return null;//end of case u

            default:
                return null;


        }//end of switch statement
    }

    /**
     * This method sets the relations between the currently finished XML
     * element(child) and its containing element in terms of Java objects.
     * Usually it involves adding the object to the parent's child object
     * list.
     *
     * @param childElement name  is the  the child element name
     * @param parent is a reference to the parent's Java object
     * @param child is the completed child object to connect to the parent
     *
     * @return true if the element was added successfully, false, if the
     *              child does not match into the parent.
     */
    public  boolean setElementRelation( String childElement, Object parent, Object child ){

        switch ( childElement.charAt( 0 ) ) {
            //a argument adag
            case 'a':
                if( child instanceof Arguments ){
                    Arguments a = (Arguments)child;
                    a.addArgument( mTextContent.toString() );

                    if( parent instanceof Job ){
                        //argument appears in job element
                        Job j = (Job)parent;
                        j.setArguments( a.toString() );
                        return true;
                    }
                }
                else if( child instanceof Map && parent == null){
                    //end of parsing reached
                    mLogger.log( "End of last element </adag> reached ",
                                  LogManager.DEBUG_MESSAGE_LEVEL );

                    this.mCallback.cbDone();
                    return true;
                }
                return false;

            //c child 
            case 'c':
                if( parent instanceof Map ){
                    if( child instanceof PCRelation ){
                        PCRelation pc = (PCRelation)child;
                        //call the callback
                        this.mCallback.cbParents( pc.getChild(), mParents);
                        return true;
                    }
                    
                }
                return false;

            //d dax dag
            case 'd':
                if( parent instanceof Map ){

                    if( child instanceof DAGJob ){
                        //dag appears in adag element
                        DAGJob dagJob = ( DAGJob )child;

                        

                        //call the callback function
                        this.mCallback.cbJob(dagJob);
                        return true;
                    }
                    else if( child instanceof DAXJob ){
                        //dag appears in adag element
                        DAXJob daxJob = ( DAXJob )child;

                        
                        //call the callback function
                        this.mCallback.cbJob( daxJob );
                        return true;
                    }
                }
                return false;


            //f file
            case 'f':
                if( child instanceof ReplicaLocation ){
                    ReplicaLocation rl = ( ReplicaLocation )child;
                    if( parent instanceof Map ){
                        //file appears in adag element
//                        this.mReplicaStore.add( rl );
                        this.mCallback.cbFile( rl );
                        return true;
                    }
                    else if( parent instanceof Arguments ){
                        //file appears in the argument element
                        Arguments a = (Arguments)parent;
                        a.addArgument( mTextContent.toString() );
                        a.addArgument( rl );
                        return true;
                    }
                }
                return false;

            //e executable
            case 'e':
                if( child instanceof TransformationCatalogEntry ){
                    if( parent instanceof Map ){
                        //executable appears in adag element
                        TransformationCatalogEntry tce = ( TransformationCatalogEntry )child;

                        //moved the callback call to end of pfn
                        //each new pfn is a new transformation
                        //catalog entry
                        //this.mCallback.cbExecutable( tce );
                        return true;
                    }
                }
                return false;

            //i invoke
            case 'i':
                if( child instanceof Invoke ){
                    Invoke i = (Invoke)child;
                    if( parent instanceof Job ){
                        //invoke appears in job element
                        unSupportedNestingOfElements( "job", "invoke" );

                        return true;
                    }
                }
                return false;

            //j job
            case 'j':
                if( child instanceof Job  && parent instanceof Map ){
                    //callback for Job
                    this.mCallback.cbJob( (Job)child );
                    return true;
                }
                return false;

            //m metadata
            case 'm':
                if ( child instanceof MetaData ) {
                    MetaData md = ( MetaData )child;
                    md.setValue( mTextContent.toString().trim() );
                    //metadata appears in file element
                    if( parent instanceof ReplicaLocation ){
                        unSupportedNestingOfElements( "file", "metadata" );
                        return true;
                    }
                    //metadata appears in executable element
                    if( parent instanceof TransformationCatalogEntry ){
                        unSupportedNestingOfElements( "executable", "metadata" );
                        return true;
                    }
                }
                return false;

            //p parent profile pfn
            case 'p':
                if( parent instanceof PCRelation ){
                    if( child instanceof String ){
                        //parent appears in child element
                        String parentNode = ( String )child;
                        PCRelation pc = (PCRelation) (( PCRelation )parent).clone();
                        pc.setParent( parentNode );
                        mParents.add( pc );
                        return true;
                    }
                }
                else if ( child instanceof Profile ){
                    Profile p = ( Profile ) child;
                    p.setProfileValue( mTextContent.toString().trim() );
                    mLogger.log( "Set Profile Value to " + p.getProfileValue(), LogManager.DEBUG_MESSAGE_LEVEL );
                    if ( parent instanceof ReplicaLocation ) {
                        //profile appears in file element
                        unSupportedNestingOfElements( "file", "profile" );
                        return true;
                    }
                    else if ( parent instanceof TransformationCatalogEntry ) {
                        //profile appears in file element
                        unSupportedNestingOfElements( "executable", "profile" );
                        return true;
                    }
                    else if ( parent instanceof Job ){
                        //profile appears in the job element
                        Job j = (Job)parent;
                        j.addProfile( p );
                        return true;
                    }
                }
                else if( child instanceof PFN ){
                    if ( parent instanceof ReplicaLocation ) {
                        //pfn appears in file element
                        ReplicaLocation rl = ( ReplicaLocation )parent;
                        PFN pfn = ( PFN )child;
                        rl.addPFN( pfn );
                        return true;
                    }
                    else if ( parent instanceof TransformationCatalogEntry){
                        //pfn appears in executable element
                        TransformationCatalogEntry tce = (TransformationCatalogEntry)parent;
                        PFN pfn = ( PFN )child;
                        tce.setResourceId( pfn.getSite() );
                        tce.setPhysicalTransformation( pfn.getURL() );
                        
                        //convert file url appropriately for installed executables
                        //before returning
                        this.mCallback.cbExecutable( Abstract.modifyForFileURLS(tce) );

                        return true;
                    }
                }
                return false;

            //s stdin stdout stderr
            case 's':
                if( parent instanceof Job ){
                    Job j = ( Job )parent;

                    if( child instanceof edu.isi.pegasus.planner.dax.File ){
                        //stdin stdout stderr appear in job element
                        edu.isi.pegasus.planner.dax.File f = ( edu.isi.pegasus.planner.dax.File )child;

                        if( childElement.equals( "stdin" ) ){
                            j.setStdIn( f.getName() );
                            return true;
                        }
                        else if( childElement.equals( "stdout" ) ){
                            j.setStdOut( f.getName() );
                            return true;
                        }

                        if( childElement.equals( "stderr" ) ){
                            j.setStdErr( f.getName() );
                            return true;
                        }
                    }
                }
                return false;

            //t transformation
            case 't':
                if( parent instanceof Map ){
                    if( child instanceof CompoundTransformation ){
                        this.mCallback.cbCompoundTransformation( (CompoundTransformation)child );
                        return true;
                    }
                    return true;
                }
                return false;

            //u uses
            case 'u':
                if( child instanceof PegasusFile ){
                    PegasusFile pf = ( PegasusFile )child;
                    if( parent instanceof Job ){
                        //uses appears in job
                        Job j = ( Job )parent;

                        if( pf.getLinkage().equals( LINKAGE.INPUT ) ){
                            j.addInputFile(pf);
                        }
                        else if( pf.getLinkage().equals( LINKAGE.OUTPUT ) ){
                            j.addOutputFile(pf);
                        }
                        else if( pf.getLinkage().equals( LINKAGE.INOUT ) ){
                            j.addInputFile(pf);
                            j.addOutputFile(pf);
                        }
                        return true;
                    }
                    else if( parent instanceof CompoundTransformation ){
                        CompoundTransformation compound = (CompoundTransformation)parent;
                        compound.addDependantFile( pf );
                        return true;
                    }
                }
                return false;

            //default case
            default:
                return false;

        }
    }

    /**
     * Returns the id for a job
     *
     * @param j the job
     *
     * @return the id.
     */
    protected String constructJobID( Job j ){
        //construct the jobname/primary key for job
        StringBuffer name = new StringBuffer();

        //prepend a job prefix to job if required
        if (mJobPrefix != null) {
            name.append(mJobPrefix);
        }

        //append the name and id recevied from dax
        name.append(j.getTXName());
        name.append("_");
        name.append(j.getLogicalID());
        return name.toString();
    }

    /**
     * Sanity check on the version that this parser works on.
     * 
     * @param version  the version as specified in the DAX
     */
    protected void sanityCheckOnVersion( String  version ) {
        if( version == null ){
            mLogger.log( "Version not specified in the adag element " ,
                         LogManager.WARNING_MESSAGE_LEVEL );
            return ;
        }
        
        //add a 0 suffix
        String nversion = version + ".0";
        if( CondorVersion.numericValue( nversion) < DAXParser3.DAX_VERSION_3_2_0 ){
            StringBuffer sb = new StringBuffer();
            sb.append( "DAXParser3 Unsupported DAX Version " ).append( version ).
               append( ". Set pegasus.schema.dax property to load the old DAXParser" );
            throw new RuntimeException( sb.toString() );
        }
        
        return;
    }

    /**
     * Private class to handle mix data content for arguments tags.
     *
     */
    private class Arguments{

        /**
        * Handle to a job arguments to handle mixed content.
         */
        protected StringBuffer mBuffer;

        /**
         * The default constructor
         */
        public Arguments(){
            reset();
        }

        /**
         * Resets the internal buffer
         */
        public void reset() {
            mBuffer = new StringBuffer();
        }

        /**
         * Adds text to the arguments string
         *
         * @param text the text to be added.
         */
        public void addArgument( String text ){
            mBuffer.append( text );
        }

        /**
         * Adds filename to the arguments
         *
         * @param rl  the ReplicaLocation object
         */
        public void addArgument(ReplicaLocation rl) {
            mBuffer.append( " " ).append( rl.getLFN() ).append( " ");
        }


        /**
         * Adds a file name to the argument string
         *
         * @param file  the file object.
         */
        private void addArgument( edu.isi.pegasus.planner.dax.File file ){
            mBuffer.append(  " " ).append( file.getName() ).append( " " );
        }


        /**
         * Our own implementation for ignorable whitespace. A String that holds the
         * contents of data passed as text by the underlying parser. The whitespaces
         * at the end are replaced by one whitespace.
         *
         * @param str   The string that contains whitespaces.
         *
         * @return  String corresponding to the trimmed version.
         *
         */
        public String ignoreWhitespace(String str) {
            return ignoreWhitespace(str, mProps.preserveParserLineBreaks());
        }

        /**
         * Our own implementation for ignorable whitespace. A String that holds the
         * contents of data passed as text by the underlying parser. The whitespaces
         * at the end are replaced by one whitespace.
         *
         * @param str   The string that contains whitespaces.
         *
         * @return  String corresponding to the trimmed version.
         *
         */
       public String ignoreWhitespace(String str, boolean preserveLineBreak ){
            boolean st = false;
            boolean end = false;
            int length = str.length();
            boolean sN = false;//start with \n ;
            boolean eN = false;//end with \n

            if(length > 0){
                sN = str.charAt(0) == '\n';
                eN = str.charAt(length -1) == '\n';
                //check for whitespace in the
                //starting
                if(str.charAt(0) == ' ' || str.charAt(0) == '\t' || str.charAt(0) == '\n'){
                    st = true;
                }
                //check for whitespace in the end
                if(str.length() > 1 &&
                    (str.charAt(length -1) == ' ' ||
                    str.charAt(length -1) == '\t' ||
                    str.charAt(length -1) == '\n')){

                    end = true;
                }
                //trim the string and add a single whitespace accordingly
                str = str.trim();
                str = st == true ? ' ' + str:str;
                str = end == true ? str + ' ':str;

                if( preserveLineBreak ){
                    str = sN ? '\n' + str:str;
                    str = eN ? str + '\n':str;
                }
            }

            return str;
        }

        /**
         * Returns the arguments as string
         *
         * @return the arguments
         */
        public String toString(){
            return this.ignoreWhitespace( mBuffer.toString() );
        }


    }

    /**
     * 
     * @param args
     */
    public static void main( String[] args ){
        LogManagerFactory.loadSingletonInstance().setLevel( 5 );
        /*DAXParser3 parser = new DAXParser3(  );
        if (args.length == 1) {
            parser.startParser( args[0] );
 
        } else {
            System.out.println("Usage: SiteCatalogParser <input site catalog xml file>");
        }*/
        
    }





    
}

