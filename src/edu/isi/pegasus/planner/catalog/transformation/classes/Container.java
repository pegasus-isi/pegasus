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

package edu.isi.pegasus.planner.catalog.transformation.classes;

import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.namespace.Pegasus;
import java.util.List;

/**
 * A container data class to use in the Transformation Catalog
 * 
 * @author Karan Vahi
 */
public class Container implements Cloneable {
    
    /**
     * The types of container supported.
     */
    public static enum TYPE{ docker, singularity };

    /**
     * Type of container to use
     */
    protected TYPE mType;
    
    /**
     * URL to image in a docker hub or a url to an existing docker
     * file exported as a tar file
     */
    protected PegasusURL mImageURL; 

     /**
      * optional site attribute to tell pegasus which site tar file
      * exists. useful for handling file URL's correctly
      */ 
    protected String mImageSite;
    
    /**
     * a url to an existing docker file to build container image  from scratch
     */
    protected PegasusURL mDefinitionFileURL;
    
    /**
     * The profiles associated with the site.
     */
    protected Profiles mProfiles;
    
    /**
     * Default constructor
     */
    public Container(){
        mType = TYPE.docker;
        mImageURL = null;
        mDefinitionFileURL = null;
        mImageSite = null;
        mProfiles = new Profiles();
    }
    
    /**
     * Set the image URL
     * 
     * @param url 
     */
    public void setImageURL( String url ){
        mImageURL = new PegasusURL( url );
    }
    
    /**
     * Return the URL to the image
     * @return 
     */
    public PegasusURL getImageURL(){
        return mImageURL;
    }
    
    /**
     * Set image definition URL
     * 
     * @param url 
     */
    public void setImageDefinitionURL( String url ){
        mDefinitionFileURL = new PegasusURL( url );
    }
    
    /**
     * Return image defintion URL
     * 
     * @return 
     */
    public PegasusURL getImageDefinitionURL(){
        return mDefinitionFileURL;
    }
    
    /**
     * Set the site where image resides
     * 
     * @param site 
     */
    public void setImageSite( String site  ){
        mImageSite = site;
    }
    
    /**
     * Return site with which image is associated
     * 
     * @return 
     */
    public String getImageSite(){
        return mImageSite;
    }
    
    /**
     * Allows you to add one profile at a time to the transformation.
     * @param profiles profiles to be added.
     */
    public void addProfiles( Profiles profiles ) {
    	if(profiles != null) {
    		if ( this.mProfiles == null ) {
    			this.mProfiles = new Profiles();
            }
    		this.mProfiles.addProfilesDirectly( profiles );
    	}
    }


    /**
     * Allows you to add one profile at a time to the transformation.
     * @param profile Profile  A single profile consisting of mNamespace, key and value
     */
    public void addProfile( Profile profile ) {
        if ( profile != null ) {
            if ( this.mProfiles == null ) {
                this.mProfiles = new Profiles();
            }
            //PM-826 allow multiple profiles with same key 
            if( profile.getProfileNamespace().equalsIgnoreCase( Pegasus.NAMESPACE_NAME ) ){
                this.mProfiles.addProfile( profile );
            }
            else{
                this.mProfiles.addProfileDirectly( profile );
            }
        }
    }

    /**
     * Returns the list of profiles associated with the transformation.
     * @return List Returns null if no profiles associated.
     */
    public List<Profile> getProfiles() {
        return ( this.mProfiles == null ) ? null : this.mProfiles.getProfiles();
    }

    /**
     * Returns the profiles for a particular Namespace.
     * @param namespace String The mNamespace of the profile
     * @return List   List of Profile objects. returns null if none are found.
     */
    public List<Profile> getProfiles( String namespace ) {
        return ( this.mProfiles == null ) ? null : mProfiles.getProfiles(namespace);
    }
    
    /**
     * Allows you to add multiple profiles to the transformation.
     * @param profiles List of Profile objects containing the profile information.
     */
    public void addProfiles( List profiles ) {
        if ( profiles != null ) {
            if ( this.mProfiles == null ) {
                this.mProfiles = new Profiles();
            }
            this.mProfiles.addProfilesDirectly( profiles );
        }
    }

    /**
     * Set the type of the container.
     * 
     * @param type 
     */
    public void setType( TYPE type ){
        this.mType = type;
    }
    
    
    /**
     * Return the type of the container.
     * 
     * @return  type 
     */
    public TYPE getType( ){
        return this.mType;
    }
    
    
    /**
     * Returns the clone of the object.
     *
     * @return the clone
     */
    public Object clone(){
        Container obj;
        try{
            obj = ( Container ) super.clone();
            obj.setType( mType );
            obj.setImageSite(mImageSite);
            
            PegasusURL url = this.getImageDefinitionURL();
            if( url != null ){
                obj.setImageDefinitionURL( url.toString() );
            }
            url = this.getImageURL();
            if( url != null ){
                obj.setImageURL( url.toString() );
            }
            //FIX me check for profiles clone
            obj.addProfiles( this.mProfiles );
            
        }
        catch( CloneNotSupportedException e ){
            //somewhere in the hierarch chain clone is not implemented
            throw new RuntimeException("Clone not implemented in the base class of " + this.getClass().getName(),
                                       e );
        }
        return obj;
    }
    
}
