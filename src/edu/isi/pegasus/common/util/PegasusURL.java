/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.common.util;

import java.io.File;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A common PegasusURL class to use by the planner and other components.
 *
 * The class parses the PegasusURL into 3 main components
 *  - protocol
 *  - hostname
 *  - path
 *
 * The class is consistent with the PegasusURL parsing scheme used in pegasus-transfer.
 *
 * @author Karan Vahi
 * @author  Mats Rynge
 */
public class PegasusURL {

    /**
     * The scheme name for file url.
     */
    public static final String FILE_URL_SCHEME = "file:";

    /**
     * The scheme name for file url.
     */
    public static final String SYMLINK_URL_SCHEME = "symlink:";
    
    
    /**
     * The scheme name for gsift url.
     */
    public static final String GSIFTP_URL_SCHEME = "gsiftp:";
    
    
    /**
     * The scheme name for S3 url.
     */
    public static final String S3_URL_SCHEME = "s3:";


    /**
     * The default protocol if none is determined from the PegasusURL or path
     */
    public static final String DEFAULT_PROTOCOL = "file";

    /**
     * singularity hub protocol scheme
     */
    public static String SINGULARITY_PROTOCOL_SCHEME = "shub";
    
    /**
     * shifter hub protocol scheme
     */
    public static String SHIFTER_PROTOCOL_SCHEME = "shifter";
    
    /**
     * Docker hub protocol scheme
     */
    public static String DOCKER_PROTOCOL_SCHEME = "docker";
    
    /**
     *
     * Stores the regular expressions necessary to parse a PegasusURL into 3 components
     * protocol, host and path
     */
    private static final String mRegexExpression = "([\\w]+)://([\\w\\.\\-:@#]*)(/?[\\S]*)";

    /**
     * Stores compiled patterns at first use, quasi-Singleton.
     */
    private static Pattern mPattern = null;

    /**
     * The protocol referred to by the PegasusURL
     */
    private String mProtocol;

    /**
     * The hostname referred to by the PegasusURL.
     * Can include the port also
     */
    private String mHost;

    /**
     * The path referred to by the PegasusURL
     */
    private String mPath;
    
    /**
     * The URL.
     */
    private String mURL;


    /**
     * The default constructor.
     */
    public PegasusURL(){
        if( mPattern == null ){
             mPattern = Pattern.compile( mRegexExpression );
         }
        reset();
    }

    /**
     * The overloaded constructor.
     *
     * @param url   the url to be parsed internally
     */
    public PegasusURL( String url ){
        this();
        this.parse( url );
    }


    /**
     * Parses the url and populates the internal member variables that can
     * be accessed via the appropriate accessor methods
     *
     * @param url
     */
    public void parse( String url ){
        //reset internal variables
        reset();

        //special case for file url's
        if( url.indexOf( ":" ) == -1 ){
            url = PegasusURL.DEFAULT_PROTOCOL + "://" + url;
        }
	mURL = url;

        Matcher m = mPattern.matcher( url );
        if( m.matches() ){
            mProtocol = m.group( 1 );
            mHost     = m.group( 2 );
            mPath     = m.group( 3 );
        }
        else{
            throw new RuntimeException( "Unable to parse URL " + url );
        }
    }

    /**
     * Returns the protocol associated with the PegasusURL
     *
     * @return  the protocol else empty
     */
    public String getProtocol(){
        return mProtocol;
    }

     /**
     * Returns the host asscoiated with the PegasusURL
     *
     * @return  the host else empty
     */
    public String getHost(){
        return mHost;
    }

    /**
     * Returns the path associated with the PegasusURL
     *
     * @return  the host else empty
     */
    public String getPath(){
        return mPath;
    }

    /**
     * Returns the url prefix associated with the PegasusURL. The PegasusURL prefix is the part
     * of the PegasusURL composed of protocol and the hostname
     *
     * For example PegasusURL prefix for
     * <pre>
     * gsiftp://dataserver.phys.uwm.edu/~/griphyn_test/ligodemo_output
     * </pre>
     *
     * is gsiftp://dataserver.phys.uwm.edu
     *
     * @return  the host else empty
     */
    public String getURLPrefix(){
        StringBuffer prefix = new StringBuffer();
        prefix.append( this.getProtocol() ).
               append( "://" ).
               append( this.getHost() );
        return prefix.toString();
    }
    
    
    /**
     * Returns the full URL denoted by this object
     *
     *
     * @return      
     */
    public String getURL(){
        return mURL;
    }

    /**
     * Resets the internal member variables
     */
    public void reset() {
        mProtocol  = "";
        mHost = "";
        mPath = "";
        mURL = "";
    }
    
    
    /**
     * Matches if two Pegasus URL objects are the same
     * If the both URL's are file URL's then it does a canonical match on the 
     * path component , else just a string match
     *
     * @return true  if URL match
     */
    public boolean equals(Object obj) {
       // null check
       if (obj == null) {
           return false;
       }

       // see if type of objects match
       if (!(obj instanceof PegasusURL)) {
           return false;
       }

       PegasusURL url = (PegasusURL) obj;
       String scheme1 = this.getProtocol();
       String scheme2 = url.getProtocol();

       //match on protocol
       boolean result = (scheme1 == null && scheme2 == null)
               || (scheme1 != null && scheme2 != null && scheme1.equals(scheme2));
       
       //match on hostname
       if( result ){
           String hostname1 = this.getHost();
           String hostname2 = url.getHost();

            //match on protocol
            result = (hostname1 == null && hostname2 == null)
                    || (hostname1 != null && hostname2 != null && hostname1.equals(hostname2));
       }
       
       //match on just the path component
       if( result ){
           String path1 = this.getPath();
           String path2 = url.getPath();

            //match on protocol
            result = (path1 != null && path2 != null);
            if( result ){
               try {
                   result = new File( path1 ).getCanonicalPath().equals( new File( path2 ).getCanonicalPath() );
               } catch (IOException ex) {
                   System.err.println( "ERROR: In Pegasus URL equal match " + ex.getMessage() );    
               }
            }
       }
       
       return result;
    }
                

    /**
     * The contents represented as a string
     *
     * @return
     */
    public String toString(){
       StringBuffer sb = new StringBuffer();
       sb.append( "url -> " ).append( this.getURL() ).append( " , " ).
          append( "protocol -> " ).append( this.getProtocol() ).append( " , " ).
          append( "host -> " ).append( this.getHost() ).append( " , " ).
          append( "path -> " ).append( this.getPath() ).append( " , " ).
          append( "url-prefix -> ").append( this.getURLPrefix() );
       return sb.toString();
    }


    /**
     * Test program
     *
     * @param args
     */
    public static void main( String[] args ){
        //should print
        //protocol -> gsiftp , host -> sukhna.isi.edu , path -> /tmp/test.file , url-prefix -> gsiftp://sukhna.isi.edu
        String url = "gsiftp://sukhna.isi.edu/tmp/test.file";
        System.out.println( url );
        System.out.println( new PegasusURL(url) );


        //should print
        //protocol -> file , host ->  , path -> /tmp/test/k , url-prefix -> file://
        url = "file:///tmp/test/k";
        System.out.println( url );
        System.out.println( new PegasusURL(url) );

        //should print
        //protocol -> gsiftp , host -> dataserver.phys.uwm.edu , path -> /~/griphyn_test/ligodemo_output/ , url-prefix -> gsiftp://dataserver.phys.uwm.edu
        url = "gsiftp://dataserver.phys.uwm.edu/~/griphyn_test/ligodemo_output/" ;
        System.out.println( url );
        System.out.println( new PegasusURL(url) );
        
        System.out.println( new PegasusURL(url).equals( new PegasusURL("gsiftp://dataserver.phys.uwm.edu/dest.php?x:=griphyn_test//ligodemo_output/")));

        //should print
        //protocol -> file , host ->  , path -> /tmp/path/to/input/file , url-prefix -> file://
        url =  "/tmp/path/to/input/file" ;
        System.out.println( url );
        System.out.println( new PegasusURL(url) );

        url =  "http://isis.isi.edu/" ;
        System.out.println( url );
        System.out.println( new PegasusURL(url) );

        url =  "http://isis.isi.edu/filename" ;
        System.out.println( url );
        System.out.println( new PegasusURL(url) );

        url =  "http://isis.isi.edu/directory/filename" ;
        System.out.println( url );
        System.out.println( new PegasusURL(url) );

    }
}
