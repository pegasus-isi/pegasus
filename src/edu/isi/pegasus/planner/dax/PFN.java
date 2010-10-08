/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.planner.dax;
import java.util.*;

import edu.isi.pegasus.common.util.XMLWriter;


/**
 *
 * @author gmehta
 */
public class PFN {

   protected String mURL;
   protected String mSite;
   protected List<Profile> mProfiles;

   public PFN(String url){
       this(url,null);
   }
   
   public PFN(String url, String site){
       mURL=url;
       mSite=site;
//       mProfiles=new Profiles();
       mProfiles=new LinkedList<Profile>();
   }

   public String getURL(){
       return mURL;
   }

   public PFN setSite(String site){
       mSite=site;
       return this;
   }
   
   public String getSite(){
       return (mSite==null) ? "" : mSite;
   }

   /**
   public PFN addProfile(String namespace, String key, String value){
       mProfiles.addProfileDirectly(namespace, key, value);
       return this;
   }

   public PFN addProfile(Profiles.NAMESPACES namespace, String key, String value){
       mProfiles.addProfileDirectly(namespace,key,value);
       return this;
   }

   public PFN addProfiles(List<Profile> profiles){
       mProfiles.addProfilesDirectly(profiles);
       return this;
   }

   public PFN addProfiles(Profiles profiles){
       mProfiles.addProfilesDirectly(profiles);
       return this;
   }
**/
   public PFN addProfile(String namespace, String key, String value){
       mProfiles.add(new Profile(namespace, key, value));
       return this;
   }

   public PFN addProfile(Profile.NAMESPACE namespace, String key, String value){
       mProfiles.add(new Profile(namespace,key,value));
       return this;
   }

   public PFN addProfiles(List<Profile> profiles){
       mProfiles.addAll(profiles);
       return this;
   }

   public PFN addProfiles(Profile profile){
       mProfiles.add(profile);
       return this;
   }
   public List<Profile> getProfiles(){
       return Collections.unmodifiableList(mProfiles);
   }

   public void toXML(XMLWriter writer){
       writer.startElement("pfn");
       writer.writeAttribute("url", mURL);
       if(mSite!=null || !mSite.equals("local")){
           writer.writeAttribute("site", mSite);
       }
       for(Profile p : mProfiles){
           p.toXML(writer);
       }
       writer.endElement();
   }
}
