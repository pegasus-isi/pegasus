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

package edu.isi.pegasus.planner.parser;


import edu.isi.pegasus.planner.parser.tokens.OpenBrace;
import edu.isi.pegasus.planner.parser.tokens.TransformationCatalogReservedWord;
import edu.isi.pegasus.planner.parser.tokens.Token;
import edu.isi.pegasus.planner.parser.tokens.QuotedString;
import edu.isi.pegasus.planner.parser.tokens.Identifier;
import edu.isi.pegasus.planner.parser.tokens.CloseBrace;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.logging.LogManager;

import edu.isi.pegasus.common.util.Version;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;

import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.catalog.transformation.impl.Abstract;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;

import java.util.logging.Level;
import java.util.logging.Logger;
import edu.isi.pegasus.planner.classes.Profile;


/**
 * Parses the input stream and generates the TransformationStore as output.
 *
 * This parser is able to parse the Transformation Catalog specification in the
 * following format
 * 
 * <pre>
  tr example::keg:1.0 { 

    #specify profiles that apply for all the sites for the transformation 
    #in each site entry the profile can be overriden 

    profile env "APP_HOME" "/tmp/myscratch"
    profile env "JAVA_HOME" "/opt/java/1.6"

    site isi-cluster {
      profile env "HELLo" "WORLD"
      profile condor "FOO" "bar"
      profile env "JAVA_HOME" "/bin/java.1.6"
      pfn "/path/to/keg"
      arch "x86"
      os "linux"
      osrelease "fc"
      osversion "4"
      
      # installed means pfn refers to path in the container.
      # stageable means the executable can be staged into the container
      type "INSTALLED" 

      # optional attribute to specify the container to use
      container "centos-pegasus"
    }
  }

  cont centos-pegasus{
    type "docker"

    # URL to image in a docker hub or a url to an existing docker
    # file exported as a tar file
     image "/URL/" 

    # optional site attribute to tell pegasus which site tar file
    # exists. useful for handling file URL's correctly
     image_site "optional site"
  
    # a url to an existing docker file to build container image  from scratch
     dockerfile "/URL"

    # specify env profile via env option do docker run
    profile env "JAVA_HOME" "/opt/java/1.6"        
  }

 </pre>
 *
 * @author Karan Vahi
 * @author Jens Vöckler
 * @version $Revision$
 *
 * @see edu.isi.pegasus.planner.parser.TransformationCatalogTextScanner
 */
public class TransformationCatalogTextParser {

    /**
     * The access to the lexical scanner is stored here.
     */
    private TransformationCatalogTextScanner mScanner = null;

    /**
     * Stores the look-ahead symbol.
     */
    private Token mLookAhead = null;

    /**
     * The transformation to the logger used to log messages.
     */
    private LogManager mLogger;


    /**
     * Initializes the parser with an input stream to read from.
     *
     * @param r is the stream opened for reading.
     * @param logger the transformation to the logger.
     *
     * @throws IOException
     * @throws ScannerException
     */
    public TransformationCatalogTextParser(Reader r, LogManager logger ) throws IOException, ScannerException {
        mLogger  = logger;
        mScanner = new TransformationCatalogTextScanner(r);
        mLookAhead = mScanner.nextToken();
    }

    /**
     * Parses the complete input stream, into the PoolConfig data object that
     * holds the contents of all the sites referred to in the stream.
     *
     *@param modifyFileURL Boolean indicating whether to modify the file URL or not
     *
     * @return TransformationStore
     *
     * @throws IOException
     * @throws ScannerException
     * @throws Exception
     * @see org.griphyn.cPlanner.classes.PoolConfig
     */
    public TransformationStore parse(boolean modifyFileURL) throws IOException,
        ScannerException  {
        //to check more
        TransformationStore store = new TransformationStore();

        try{
            String transformation   = null;
            do {
                if ( mLookAhead != null ) {
                    
                    if (! ( mLookAhead instanceof TransformationCatalogReservedWord ) ){ 
                        throw new ScannerException( mScanner.getLineNumber(),
                                          "expecting reserved word \"tr\" or \"cont\" . Instead got \"" +
                                                  (  mLookAhead ) + "\"");
                    }
                    else if(  ( (TransformationCatalogReservedWord) mLookAhead ).getValue() ==
                                TransformationCatalogReservedWord.TRANSFORMATION ) {
                    
                        //get the  transformation/id, that is parsed differently
                        //compared to the rest of the attributes of the name.
                        transformation = getTransformation();


                        //check for any profiles that maybe specified and need to
                        //applied for all entries related to the transformation
                        Profiles profiles = getProfilesForTransformation();

                        while( !( mLookAhead instanceof CloseBrace ) ){
                            TransformationCatalogEntry entry = getTransformationCatalogEntry( transformation, profiles , modifyFileURL);
                            store.addEntry( entry );
                            //we have information about one transformation catalog container
                            mLogger.log( "Transformation Catalog Entry parsed is - " + entry,
                                      LogManager.DEBUG_MESSAGE_LEVEL);
                        }

                        //again check for any profiles that may be associated
                        //makes profiles overloading slightly more complicated
                        //no need to do it
                        //profiles.addAll( getProfilesForTransformation() );

                        if (! (mLookAhead instanceof CloseBrace)) {
                            throw new ScannerException( mScanner.getLineNumber(),
                                                       "expecting a closing brace");
                        }
                    }
                    else if(  ( (TransformationCatalogReservedWord) mLookAhead ).getValue() ==
                                TransformationCatalogReservedWord.CONT ) {
                    
                        //get the  transformation/id, that is parsed differently
                        String name = getEntryIdentifier( TransformationCatalogReservedWord.symbolTable().get("cont") );
                        
                        Container entry = getContainerEntry( name, modifyFileURL);
                        //store.addEntry( container );
                        //we have information about one transformation catalog container
                        mLogger.log( "Container Entry parsed is - " + entry,
                                  LogManager.DEBUG_MESSAGE_LEVEL);
                        store.addContainer(entry);

                    }
                    else{
                        throw new ScannerException( mScanner.getLineNumber(),
                                          "expecting reserved word \"tr\" or \"cont\" . Instead got \"" +
                                                  ( (TransformationCatalogReservedWord) mLookAhead ) + "\"");
                    }

                    mLookAhead = mScanner.nextToken();

                
                }
            }
            while ( mScanner.hasMoreTokens() );
        }
        //we wrap all non scanner and ioexceptions as scanner exceptions
        catch( ScannerException e ){
            throw e;
        }
        catch( IOException e ){
            throw e;
        }
        catch( Exception e ){
            //wrap as a scanner exception and throw
            throw new ScannerException( mScanner.getLineNumber(), e );
        }

        //PM-1165 resolve container references
        store.resolveContainerReferences();
        
        return store;
    }



    /**
     * Remove potential leading and trainling quotes from a string.
     *
     * @param input is a string which may have leading and trailing quotes
     * @return a string that is either identical to the input, or a
     * substring thereof.
     */
    public String niceString(String input) {
        // sanity
        if (input == null) {
            return input;
        }
        int l = input.length();
        if (l < 2) {
            return input;
        }

        // check for leading/trailing quotes
        if (input.charAt(0) == '"' && input.charAt(l - 1) == '"') {
            return input.substring(1, l - 1);
        }
        else {
            return input;
        }
    }

    /**
     * Constructs a single transformation catalog container and returns it.
     *
     * @param entry the <code>TransformationCatalogEntry<code> object that is to be populated.
     * @param profiles  the profiles that apply to all the entries
     * @param modifyFileURL Boolean indicating whether to modify the file URL or not
     * @return  the transformation catalog container object.
     *
     * @throws even more mystery
     */
    private TransformationCatalogEntry getTransformationCatalogEntry( String transformation,  Profiles profiles , boolean modifyFileURL ) throws IOException,
        ScannerException {


        TransformationCatalogEntry entry = new TransformationCatalogEntry();
        String site = getEntryIdentifier( TransformationCatalogReservedWord.symbolTable().get("site") );
        entry.setLogicalTransformation( transformation );
        entry.setResourceId( site );


        SysInfo sysinfo = new SysInfo();
        Profiles p = (Profiles) profiles.clone();

        while ( mLookAhead != null && ! (mLookAhead instanceof CloseBrace) ) {
           

            
            //populate all the rest of the attributes
            //associated with the transformation
            if (! (mLookAhead instanceof TransformationCatalogReservedWord)) {
                throw new ScannerException(mScanner.getLineNumber(),
                    "expecting a reserved word describing a transformation attribute instead of "+
                    mLookAhead);
            }

            int word = ( (TransformationCatalogReservedWord) mLookAhead).getValue();
            String attribute = ((TransformationCatalogReservedWord)mLookAhead).toString();
            mLookAhead = mScanner.nextToken();
            
            String value = null;
            switch ( word ) {

                case TransformationCatalogReservedWord.ARCH:
                    value = getQuotedValue( "arch" );
                    sysinfo.setArchitecture( SysInfo.Architecture.valueOf( value ) );
                break;

                case TransformationCatalogReservedWord.OS:
                    value = getQuotedValue( "os" );
                    sysinfo.setOS( SysInfo.OS.valueOf( value.toLowerCase() ) );
                break;
                
                case TransformationCatalogReservedWord.OSRELEASE:
                    value = getQuotedValue( "osrelease" );
                    sysinfo.setOSRelease(value);
                break;
                
                case TransformationCatalogReservedWord.OSVERSION:
                    value = getQuotedValue( "osversion" );
                    sysinfo.setOSVersion( value );
                break;

                case TransformationCatalogReservedWord.PFN:
                    value = getQuotedValue( "pfn" );
                    entry.setPhysicalTransformation( value );
                break;

                case TransformationCatalogReservedWord.PROFILE:
                    p.addProfileDirectly( this.getProfile() );
                    break;

                case TransformationCatalogReservedWord.METADATA:
                    p.addProfileDirectly( this.getProfile( Profile.METADATA) );
                    break;
                    
                case TransformationCatalogReservedWord.TYPE:
                    value = getQuotedValue( "type" );
                    entry.setType( TCType.valueOf(value.toUpperCase()) );
                    break;
                   
                case TransformationCatalogReservedWord.CONTAINER:
                    value = getQuotedValue( "container" );
                    entry.setContainer( new Container( value ));
                    break;
                    
                default:
                    throw new ScannerException(mScanner.getLineNumber(),
                        "invalid reserved word " + attribute + " used to configure a transformation catalog entry");

            }
        }

        //System.out.println( "*** Profiles are " + p );

        entry.setSysInfo( sysinfo );
        //add all the profiles for the container only if they are empty
        if( !p.isEmpty() ){
           entry.addProfiles( p );
        }

        if (! (mLookAhead instanceof CloseBrace)) {
                        throw new ScannerException(mScanner.getLineNumber(),
                                                      "expecting a closing brace");
        }

        mLookAhead = mScanner.nextToken();
        
        //modify the container to handle for file URL's
        //specified for the PFN's
        if(modifyFileURL){
        	return Abstract.modifyForFileURLS( entry );
        }else{
        	return entry;
        }
    }

    /**
     * Returns the  transformation name, and moves the scanner to hold the next
     * <code>TransformationCatalogReservedWord</code>.
     *
     * @return  the transformation name
     *
     * @throws plenty
     */
    private String getTransformation() throws IOException,
        ScannerException {
        String transformation = null;
        /*
        if (! ( mLookAhead instanceof TransformationCatalogReservedWord ) ||
            ( (TransformationCatalogReservedWord) mLookAhead ).getValue() !=
            TransformationCatalogReservedWord.TRANSFORMATION ) {
            throw new ScannerException( mScanner.getLineNumber(),
                                          "expecting reserved word \"tr\" . Instead got \"" +
                                                  ( (TransformationCatalogReservedWord) mLookAhead ) + "\"");
        }
        */
        mLookAhead = mScanner.nextToken();

        // proceed with next token
        if (! (mLookAhead instanceof Identifier)) {
            throw new ScannerException(mScanner.getLineNumber(),
                "expecting the transformation identifier");
        }

        transformation = ( (Identifier) mLookAhead).getValue();
        mLookAhead = mScanner.nextToken();

        // proceed with next token
        if (! (mLookAhead instanceof OpenBrace)) {
            throw new ScannerException(mScanner.getLineNumber(),
                                          "expecting an opening brace");
        }
        mLookAhead = mScanner.nextToken();
        return transformation;
    }

    /**
     * Returns the name of an entry identifier where entry is demarcated by 
     * curly braces
     * 
     * 
     * @return  the transformation name
     *
     * @throws plenty
     */
    private String getEntryIdentifier (TransformationCatalogReservedWord reservedWord ) throws IOException,
        ScannerException {
        String identifier = null;
        if (! ( mLookAhead instanceof TransformationCatalogReservedWord ) ||
            ( (TransformationCatalogReservedWord) mLookAhead ).getValue() != reservedWord.getValue()
                ) {
            throw new ScannerException( mScanner.getLineNumber(),
                                          "expecting reserved word \"" + reservedWord +"\" or closing brace instead of " + mLookAhead );
        }
        mLookAhead = mScanner.nextToken();

        // proceed with next token
        if (! (mLookAhead instanceof Identifier)) {
            throw new ScannerException(mScanner.getLineNumber(),
                "expecting the reserved word \"" + reservedWord +"\" identifier");
        }

        identifier = ( (Identifier) mLookAhead).getValue();
        mLookAhead = mScanner.nextToken();

        // proceed with next token
        if (! (mLookAhead instanceof OpenBrace)) {
            throw new ScannerException(mScanner.getLineNumber(),
                                          "expecting an opening brace");
        }
        mLookAhead = mScanner.nextToken();
        return identifier;
    }


    
    /**
     * Returns a list of profiles that have to be applied to the entries for
     * all the sites corresponding to a transformation.
     *
     * @return Profiles specified
     *
     * @throws IOException
     * @throws ScannerException
     */
    private Profiles getProfilesForTransformation() throws IOException,
        ScannerException {
        Profiles profiles = new Profiles();

        while( true ){
            if ( mLookAhead instanceof TransformationCatalogReservedWord ){
                int tokenValue = (( TransformationCatalogReservedWord) mLookAhead ).getValue();
                if( tokenValue == TransformationCatalogReservedWord.PROFILE  ) {
                    //move cursor to next token
                    mLookAhead = mScanner.nextToken();
                    profiles.addProfile( this.getProfile() );
                }
                else if( tokenValue == TransformationCatalogReservedWord.METADATA ){
                    //move cursor to next token
                    mLookAhead = mScanner.nextToken();
                    profiles.addProfile( this.getProfile( Profile.METADATA ) );
                }
                else{
                    break;
                }
            }
            else{
                break;
            }
        }

        return profiles;
    }

    
    /**
     * Constructs a single container object and returns it.
     *
     * @param name identifier for the container.
     * @param modifyFileURL Boolean indicating whether to modify the file URL or not
     * @return  Container container object.
     *
     * @throws even more mystery
     */
    private Container getContainerEntry( String name, boolean modifyFileURL ) throws IOException,
        ScannerException {


        Container container = new Container();
        container.setName(name);
        

        while ( mLookAhead != null && ! (mLookAhead instanceof CloseBrace) ) {
           
            //populate all the rest of the attributes
            //associated with the transformation
            if (! (mLookAhead instanceof TransformationCatalogReservedWord)) {
                throw new ScannerException(mScanner.getLineNumber(),
                    "expecting a reserved word describing a transformation attribute instead of "+
                    (TransformationCatalogReservedWord)mLookAhead);
            }

            int word = ( (TransformationCatalogReservedWord) mLookAhead).getValue();
            String attribute = ((TransformationCatalogReservedWord)mLookAhead).toString();
            mLookAhead = mScanner.nextToken();
            
            String value = null;
            switch ( word ) {

                case TransformationCatalogReservedWord.TYPE:
                    value = getQuotedValue( "type" );
                    container.setType( Container.TYPE.valueOf( value ));
                break;
                    
                case TransformationCatalogReservedWord.IMAGE:
                    value = getQuotedValue( "image" );
                    container.setImageURL(value);
                break;

                case TransformationCatalogReservedWord.IMAGE_SITE:
                    value = getQuotedValue( "image_site" );
                    container.setImageSite( value );
                break;
                
                case TransformationCatalogReservedWord.DOCKERFILE:
                    value = getQuotedValue( "dockerfile" );
                    container.setImageDefinitionURL(value);
                break;
                
                case TransformationCatalogReservedWord.PROFILE:
                    container.addProfile(this.getProfile() );
                    break;
                    
                case TransformationCatalogReservedWord.MOUNT_POINT:
                    value = getQuotedValue( "mount" );
                    container.addMountPoint(value);
                break;
                    
                default:
                    throw new ScannerException(mScanner.getLineNumber(),
                        "invalid reserved word " + attribute + " used to configure a container entry");

            }
        }


        if (! (mLookAhead instanceof CloseBrace)) {
                        throw new ScannerException(mScanner.getLineNumber(),
                                                      "expecting a closing brace");
        }

        return container;
    }
    
    /**
     * Parses a single line and returns a profile.
     *
     * @return Profile
     * @throws ScannerException
     */
    private Profile getProfile() throws ScannerException, IOException{
        return this.getProfile( null );
    }
    
    /**
     * Parses a single line and returns a metadata profile of type namespace.
     * If namespace is null, then assumes that namespace has to be parsed.
     *
     * @return Profile
     * @throws ScannerException
     */
    private Profile getProfile( String namespace ) throws ScannerException, IOException{
    
        if( namespace == null ){
            //parse the namespace from the stream
            if( !(mLookAhead instanceof Identifier) ){
                throw new ScannerException(mScanner.getLineNumber(),
                            "the \"profile\" requires a namespace identifier as first argument");
            }
            namespace = ( (Identifier) mLookAhead).getValue();
            mLookAhead = mScanner.nextToken();
        }
        
        if( !Profile.namespaceValid(namespace) ){
             throw new ScannerException( mScanner.getLineNumber(),
                                        "Invalid namespace specified for profile " + namespace );
         }

         //  System.out.println("profile namespace="+namespace );
         if (! (mLookAhead instanceof QuotedString)) {
            throw new ScannerException( mScanner.getLineNumber(),
                                        "the \"profile\"  key needs to be quoted");
         }
         String key = ( (QuotedString) mLookAhead).getValue();

                //   System.out.println("key="+((QuotedString) mLookAhead).getValue() );
         mLookAhead = mScanner.nextToken();
         if (! (mLookAhead instanceof QuotedString)) {
                    throw new ScannerException(mScanner.getLineNumber(),
                        "the \"profile\" value requires a quoted string argument");
         }
         String value = ( (QuotedString) mLookAhead).getValue();

         mLookAhead = mScanner.nextToken();
         return new Profile(namespace, niceString(key), niceString(value));
    }

    /**
     * Parses a quoted value and strips out the enclosing quotes.
     *
     * @param  key    the key for which we need to associated the quoted value
     *
     * @return quoted value.
     */
    private String getQuotedValue( String key ) throws IOException {

         //mLookAhead = mScanner.nextToken();
         //System.out.println( mLookAhead );


         // System.out.println("universe="+universe );
         if (! (mLookAhead instanceof QuotedString) ) {
             StringBuffer error = new StringBuffer();
             error.append( "The " ).append( key  ).append( " requires a quoted string as second argument " );
             throw new ScannerException( mScanner.getLineNumber(), error.toString() );

         }
         String value = niceString( ( (QuotedString)mLookAhead ).getValue());
         mLookAhead = mScanner.nextToken();

         return value;

    }




    /**
     * Test function.
     *
     * @param args
     */
    public static void main( String[] args ) throws ScannerException{
        try {
            Reader r = new FileReader(new File("/lfs1/work/pegasus-features/text-tc/sample_tc.data"));

            LogManager logger = LogManagerFactory.loadSingletonInstance();
            logger.setLevel( LogManager.DEBUG_MESSAGE_LEVEL );
            logger.logEventStart( "event.pegasus.catalog.transformation.test", "planner.version", Version.instance().toString() );


            TransformationCatalogTextParser p = new TransformationCatalogTextParser( r, logger );
            p.parse(true);

        } catch (FileNotFoundException ex) {
            Logger.getLogger(TransformationCatalogTextParser.class.getName()).log(Level.SEVERE, null, ex);
        }
        catch( ScannerException se ){
            se.printStackTrace();
        }
        catch( IOException ioe ){
            ioe.printStackTrace();
        }

    }
}
