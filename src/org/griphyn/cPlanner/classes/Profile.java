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

import edu.isi.pegasus.common.util.Escape;

import java.util.ArrayList;
import java.util.List;


/**
 * This Class hold informations about profiles associated with a tc.</p>
 *
 * @author Gaurang Mehta gmehta@isi.edu
 * @author Karan Vahi vahi@isi.edu
 *
 * @version $Revision$
 */
public class Profile
    extends Data {

    
    /**
     * A private static handle to the escape class.
     */
    private static Escape mEscape = new Escape();

    public static final String CONDOR = "condor";

    public static final String GLOBUS = "globus";

    public static final String VDS    = "pegasus";

    public static final String DAGMAN = "dagman";

    public static final String HINTS  = "hints";

    public static final String ENV    = "env";

    private String mNamespace;

    private String mKey ;

    private String mValue;

    /**
     * Returns the unknown namespace message.
     * 
     * @param namespace  the namespace.
     * @return the message 
     */
    public static final String unknownNamespaceMessage( String namespace ){
        StringBuffer sb = new StringBuffer();
        sb.append( "Unknown namespace type " ).append( namespace ).
           append( " . Valid types are " ).append( validTypesToString() );
        return sb.toString();
    }
                                        
    
    /**
     * Returns a comma separated string containing the valid namespace types.
     *
     * @return  comma separated list.
     */
    public static String validTypesToString( ){
        StringBuffer sb = new StringBuffer();
        sb.append( CONDOR ).append( ',' ).append( GLOBUS ).append( ',' ).
           append( VDS ).append( ',' ).append( DAGMAN ).append( ',' ).
           append( HINTS ).append( ',' ).append( ENV );
       return sb.toString();
    }


    /**
     *
     * C'tpr for the class;
     * @throws java.lang.Exception
     */
    public Profile() {
        mNamespace = null;
        mKey = null;
        mValue = null;

    }

    /**
     * This constructor allows to set the namespace , key and value of the PoolProfile.
     *
     * @param namespace Takes a String as the namespace. Has to be one of the predefined types.
     * @param key Takes a String as the key.
     * @param value The value for the key as String
     * @throws Exception
     */
    public Profile( String namespace, String key, String value ) {
        if ( namespaceValid( namespace ) ){
            mNamespace = new String( namespace );
            mKey = new String( key );
            mValue = new String( value );
        } else {
            throw new RuntimeException( unknownNamespaceMessage( namespace ) );
        }
    }

    /**
     * Returns a boolean indicating whether the namespace is valid or not.
     * 
     * @param namespace the namespace
     * 
     * @return true if valid namespace
     */
    public boolean namespaceValid( String namespace ){
        return ( namespace.equalsIgnoreCase( CONDOR ) ||
            namespace.equalsIgnoreCase( GLOBUS ) ||
            namespace.equalsIgnoreCase( VDS ) ||
            namespace.equalsIgnoreCase( DAGMAN ) ||
            namespace.equalsIgnoreCase( HINTS ) ||
            namespace.equalsIgnoreCase( ENV ) ) ;
    }
    
    

    /**
     * This method allows to set the namespace , key and value of the Profile.
     *
     * @param namespace Takes a String as the namespace. Has to be one of the predefined types.
     * @param key Takes a String as the key.
     * @param value The value for the key as String
     * 
     * @throws Exception
     */

    public void setProfile( String namespace, String key, String value ) {
        if ( namespaceValid( namespace ) ){
            mNamespace = new String( namespace );
            mKey = new String( key );
            mValue = new String( value );
        } else {
            throw new RuntimeException( unknownNamespaceMessage( namespace ) );
        }
    }

    /**
     * Returns the Profile (namespace, value and key);
     * @return ArrayList
     */
    public List getProfile() {
        ArrayList m_profile = new ArrayList( 3 );
        m_profile.add( mNamespace );
        m_profile.add( mKey );
        m_profile.add( mValue );
        return m_profile;
    }

    /**
     * Sets  the NameSpace of the Profile
     *
     * @param namespace the namespace
     * 
     * @exception in case of invalid namespace
     */
    public void setProfileNamespace( String namespace ) {
        if( namespaceValid( namespace ) ){
            mNamespace = namespace;
            return;
        }
        else {
            throw new RuntimeException( unknownNamespaceMessage( namespace ) );
        }
    }
    
    /**
     * Returns the NameSpace of the Profile
     * @return String
     */
    public String getProfileNamespace() {
        return mNamespace;
    }
    
    /**
     * Sets the profile key
     * 
     * @param key  the profile key
     */
    public void setProfileKey( String key ) {
        mKey = key;
    }


    /**
     * Returns the Key of the Profile
     * @return String
     */
    public String getProfileKey() {
        return mKey;
    }

    /**
     * Sets the profile value
     * 
     * @param value  the profile value
     */
    public void setProfileValue( String value ) {
        mValue = value;
    }
    
    /**
     * Returns the Value for the profile
     * @return String
     */
    public String getProfileValue() {
        return mValue;
    }

    /**
     * Returns the textual description of the  contents of <code>Profile</code>
     * object in the multiline format.
     *
     * @return the textual description in multiline format.
     */
    public String toMultiLine() {
        return this.toString();
    }


    /**
     * This method returns a string of the contents of this object.
     * The values are always escaped.
     *
     * @return String
     * @see org.griphyn.common.util.Escape
     */
    public String toString() {
        String output = "profile  " + mNamespace + " \"" + mKey +
            "\" \"" + mEscape.escape(mValue) + "\"";
        // System.out.println(output);
        return output;
    }

    /**
     * This method returns an xml of the contents of this object.
     * @return String.
     */
    public String toXML() {
        String output = "<profile namespace=\"" + mNamespace + "\" key=\"" +
            mKey + "\" >" + mValue + "</profile>";
        // System.out.println(output);
        return output;

    }

    /**
     * Returns a copy of the object.
     *
     * @return copy of the object.
     */
    public Object clone() {
        Profile newprofile=null;
        try {
            newprofile = new Profile( mNamespace, mKey, mValue );

        } catch ( Exception e ) {
        }
        return newprofile;
    }


}
