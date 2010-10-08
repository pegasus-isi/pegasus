/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.isi.pegasus.planner.dax;
import java.util.*;

import edu.isi.pegasus.common.util.XMLWriter;

//import org.griphyn.cPlanner.classes.Profile;

/**
 *
 * @author gmehta
 */
public class CatalogType {

    protected List<Profile> mProfiles;
    protected List<MetaData> mMetadata;
    protected List<PFN> mPFNs;

    protected CatalogType(){
        mProfiles=new LinkedList<Profile>();
        mMetadata=new LinkedList<MetaData>();
        mPFNs=new LinkedList<PFN>();

    }


   public CatalogType addPhysicalFile(String url){
       PFN p = new PFN(url);
       mPFNs.add(p);
       return this;
   }

   public CatalogType addPhysicalFile(String url, String site){
       PFN p = new PFN(url,site);
           mPFNs.add(p);
           return this;
               }

   public CatalogType addPhysicaFile(PFN pfn){
       mPFNs.add(pfn);
       return this;
   }

   public CatalogType addPhysicalFiles(List<PFN> pfns){
       mPFNs.addAll(pfns);
       return this;
   }

   public List<PFN> getPhysicalFiles(){
       return Collections.unmodifiableList(mPFNs);
   }

   public CatalogType addMetaData(String type, String key, String value){
       MetaData m = new MetaData(type,key,value);
       mMetadata.add(m);
       return this;
   }

   public CatalogType addMetaData(MetaData metadata){
       mMetadata.add(metadata);
       return this;
   }

   public CatalogType addMetaData(List<MetaData> metadata){
       mMetadata.addAll(metadata);
       return this;
   }

   public List<MetaData> getMetaData(){
       return Collections.unmodifiableList(mMetadata);
   }

 /**
  public CatalogType addProfile(String namespace, String key, String value){
       mProfiles.addProfileDirectly(namespace, key, value);
       return this;
   }

   public CatalogType addProfile(Profiles.NAMESPACES namespace, String key, String value){
       mProfiles.addProfileDirectly(namespace,key,value);
       return this;
   }

   public CatalogType addProfiles(List<Profile> profiles){
       mProfiles.addProfilesDirectly(profiles);
       return this;
   }

   public CatalogType addProfiles(Profiles profiles){
       mProfiles.addProfilesDirectly(profiles);
       return this;
   }

   public Profiles getProfiles(){
       return mProfiles;
   }
**/
    public CatalogType addProfile(String namespace, String key, String value){
       mProfiles.add(new Profile(namespace, key, value));
       return this;
   }

   public CatalogType addProfile(Profile.NAMESPACE namespace, String key, String value){
       mProfiles.add(new Profile(namespace,key,value));
       return this;
   }

   public CatalogType addProfiles(List<Profile> profiles){
       mProfiles.addAll(profiles);
       return this;
   }

   public CatalogType addProfiles(Profile profile){
       mProfiles.add(profile);
       return this;
   }

   public List<Profile> getProfiles(){
       return Collections.unmodifiableList(mProfiles);
   }
   public void toXML(XMLWriter writer){
        for(Profile p: mProfiles){
            p.toXML(writer);
        }
        for(MetaData m : mMetadata){
            m.toXML(writer);
        }
        for(PFN f: mPFNs){
            f.toXML(writer);
        }
   }

}
