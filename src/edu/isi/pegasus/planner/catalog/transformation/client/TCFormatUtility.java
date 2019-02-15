package edu.isi.pegasus.planner.catalog.transformation.client;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;

import edu.isi.pegasus.common.util.Currently;
import edu.isi.pegasus.common.util.XMLWriter;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.dax.Executable;
import edu.isi.pegasus.planner.dax.Executable.ARCH;
import edu.isi.pegasus.planner.dax.Executable.OS;
import edu.isi.pegasus.planner.dax.PFN;
import edu.isi.pegasus.planner.parser.tokens.TransformationCatalogKeywords;

/**
 * This is a utility class for converting transformation catalog into different formats.
 *
 * @author prasanth@isi.edu
 * @version $Revision $
 */

public class TCFormatUtility {
	
	/**
	 * Converts the transformations into multi line text format
	 * @param mTCStore the transformation store 
	 * @return the text format
	 */
	public static String toTextFormat(TransformationStore mTCStore){
		String newline = System.getProperty("line.separator", "\r\n");
		 String indent = "";
		 StringBuffer buf = new StringBuffer();
		 String newIndent = indent + "\t";
		 
		 // write header
		 buf.append( "# multiple line text-based transformation catalog: " +
		              Currently.iso8601(false,true,true,new Date()) );
		 buf.append( newline );
		
		 // write out data
		 //traverse through all the logical transformations in the
		 //catalog
		 for ( Iterator i= mTCStore.getTransformations(null, null).iterator(); i.hasNext(); ) {
		     //transformation is the complete name comprised of namespace,name,version
		 String transformation = (String) i.next();
		 
		 buf.append( indent );
		 buf.append( "tr " );
		 buf.append( transformation );
		 buf.append( " {"  );
		 buf.append( newline );
		
		 //get all the entries for that transformations on all sites
		 for( TransformationCatalogEntry entry: mTCStore.getEntries( transformation, (String)null ) ){
		     //write out all the entries for the transformation
		         buf.append(  toText( entry, newline, newIndent ) );
		     }
		 
		     
		     buf.append( indent );
		     buf.append( "}"  );
		     buf.append( newline );
		     buf.append( newline );
		 }
		 
		 return buf.toString();
	}
	
	
	/**
	 * Converts the transformation catalog entry object to the multi line
	 * textual representation. e.g.
	 * 
	 * site wind {
	 *   profile env "me" "with"
	 *   profile condor "more" "test"
	 *   pfn "/path/to/keg"
	 *   arch  "x86"
	 *   os    "linux"
	 *   osrelease "fc"
	 *   osversion "4"
	 *   type "STAGEABLE"
	 *  }
	 * 
	 * @param entry   the transformation catalog entry
	 * @param newline the newline characters
	 * @param indent  the indentation to use
	 * 
	 * @return the textual description
	 */
	private static String toText( TransformationCatalogEntry entry, String newline , String indent ){
	    StringBuffer sb = new StringBuffer();
	    indent = (indent != null && indent.length() > 0 ) ?
	             indent:
	             "";
	    String newIndent = indent + "\t";
	    
	    sb.append( indent );
	    sb.append( "site" ).append( " " ).append( entry.getResourceId() ).append( " {" ).append( newline );
	            
	    //list out all the profiles
	    List<Profile> profiles = entry.getProfiles();
	    if( profiles != null ){
	        for( Profile p : profiles ){
	            sb.append( newIndent ).append( "profile" ).append( " " ).
	               append( p.getProfileNamespace() ).append( " " ).
	               append( quote( p.getProfileKey() ) ).append( " ").
	               append( quote( p.getProfileValue())).append( " ").
	               append( newline );
	        }
	    }
	    
	    //write out the pfn
	    addLineToText( sb, newIndent, newline, "pfn", entry.getPhysicalTransformation() );
	    
	    //write out sysinfo
	    SysInfo s = entry.getSysInfo();
	    SysInfo.Architecture arch = s.getArchitecture();
	    if( arch != null ){
	        addLineToText( sb, newIndent, newline, "arch", arch.toString() );
	    }
	    SysInfo.OS os = s.getOS();
	    if( os != null ){
	        addLineToText( sb, newIndent, newline, "os", os.toString() );
	    }
	    String osrelease = s.getOSRelease();
	    if( osrelease != null && osrelease.length() > 0 ){
	        addLineToText( sb, newIndent, newline, "osrelease", osrelease );
	    }
	    String osversion = s.getOSVersion();
	    if( osversion != null && osversion.length() > 0 ){
	        addLineToText( sb, newIndent, newline, "osversion", osversion );
	    }
	    String glibc = s.getGlibc();
	    if( glibc != null && glibc.length() > 0 ){
	        addLineToText( sb, newIndent, newline, "glibc", glibc );
	    }
	    
	    //write out the type
	    addLineToText( sb, newIndent, newline, "type", entry.getType().toString() );
	    
	            
	    sb.append( indent ).append( "}" ).append( newline );
	    
	    return sb.toString();
	}

	/**
	 * Convenience method to add a line to the internal textual representation.
	 * 
	 * @param sb         the StringBuffer to which contents are to be added.
	 * @param newIndent  the indentation
	 * @paran newline    the newline character
	 * @param key        the key
	 * @param value      the value
	 */
	private static void addLineToText( StringBuffer sb, 
	                              String newIndent, 
	                              String newline,
	                              String key, 
	                              String value) {
	    
	    sb.append( newIndent ).append( key ).append( " " ).
	       append( quote( value ) ).append( newline );
	}

	/**
	 * Quotes a String.
	 * 
	 * @param str  the String to be quoted.
	 * 
	 * @return quoted version
	 */
	private static String quote( String str ){
	    //maybe should use the escape class also?
	    StringBuffer sb = new StringBuffer();
	    sb.append( "\"" ).append( str ).append( "\"" );
	    return sb.toString();
	}
	
	/**
	 * Prints the transformations in XML format
	 * @param tStore the transformation store
	 */
	// Note : xml format ignores logical profiles associated with a transformation.
	public static void printXMLFormat(TransformationStore tStore){
		BufferedWriter pw = new BufferedWriter(new OutputStreamWriter(System.out));
		XMLWriter writer = new XMLWriter(pw);
		for(TransformationCatalogEntry entry :tStore.getEntries(null, (TCType)null) ){
			Executable exec = new Executable(entry.getLogicalNamespace(), entry.getLogicalName(), entry.getLogicalVersion());
	        exec.setArchitecture(ARCH.valueOf(entry.getSysInfo().getArchitecture().toString()));
	        exec.setOS(OS.valueOf(entry.getSysInfo().getOS().toString()));
	        exec.setOSVersion(entry.getSysInfo().getOSVersion());
	        exec.setGlibc(entry.getSysInfo().getGlibc());
	        if(entry.getType().equals(TCType.INSTALLED)){
	        	exec.setInstalled(true);
	        }else{
	        	exec.setInstalled(false);
	        }
	        PFN pfn = new PFN(entry.getPhysicalTransformation() , entry.getResourceId());
	        if(entry.getProfiles() != null){
	        	for (Profile profile:((List<Profile>)entry.getProfiles()) ){
		        	pfn.addProfile(profile.getProfileNamespace(), profile.getProfileKey(), profile.getProfileValue());
		        }
	        }
	        
	        exec.addPhysicalFile(pfn);
	        exec.toXML(writer);
		}
	     writer.close();   
        return;
		
	}

	@SuppressWarnings("unchecked")
	public static void toYAMLFormat(TransformationStore mTCStore, Writer out) {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		try {
			List<Object> transformationData = new LinkedList<Object>();

			List<TransformationCatalogEntry> entries = mTCStore.getAllEntries();

			Map<String, Object> transformationMap = new LinkedHashMap<>();

			List<Map<String, Object>> transformations = new LinkedList<Map<String, Object>>();

			transformationMap.put("transformations", transformations);

			Map<String, Object> containerMap = new LinkedHashMap<>();

			List<Map<String, Object>> containers = new LinkedList<Map<String, Object>>();

			containerMap.put("cont", containers);

			Set<Container> containerInfo = new HashSet<Container>();

			for (TransformationCatalogEntry entry : entries) {

				Map<String, Object> entryMap = getEntryMap(transformations, entry.getLogicalName());

				Container container = entry.getContainer();
				
				if(container != null) {
					containerInfo.add(container);
				}

				if (entryMap.isEmpty()) {
					
					String nameSpace = entry.getLogicalNamespace();

					if (nameSpace != null) {
						entryMap.put(TransformationCatalogKeywords.NAMESPACE.getReservedName(), nameSpace);
					}

					String name = entry.getLogicalName();

					if (name != null) {
						entryMap.put(TransformationCatalogKeywords.NAME.getReservedName(), name);
					}

					String version = entry.getLogicalVersion();

					if (version != null) {
						Double versionDobule = Double.valueOf(version);
						entryMap.put(TransformationCatalogKeywords.VERSION.getReservedName(), versionDobule);
					}

					List<Profile> profiles = entry.getProfiles();

					if (profiles != null) {
						Object profileData = buildProfiles(profiles, false);
						if (profileData != null) {
							entryMap.put(TransformationCatalogKeywords.PROFILE.getReservedName(),
									buildProfiles(profiles, false));
						}
						Object metaData = buildProfiles(profiles, true);
						if (metaData != null) {
							entryMap.put(TransformationCatalogKeywords.METADATA.getReservedName(), metaData);
						}
					}
				}
				Object siteData = entryMap.get(TransformationCatalogKeywords.SITE.getReservedName());
				if (siteData == null) {
					siteData = new LinkedList<Map<String, Object>>();
					entryMap.put(TransformationCatalogKeywords.SITE.getReservedName(), siteData);
				}
				LinkedList<Map<String, Object>> siteList = (LinkedList<Map<String, Object>>) siteData;
				siteList.add(buildSite(entry.getResourceId(), entry.getPhysicalTransformation(), container,
						entry.getSysInfo(), entry.getType()));
				entryMap.put(TransformationCatalogKeywords.SITE.getReservedName(), siteList);
				transformations.add(entryMap);
			}
			for (Container container : containerInfo) {
				HashMap<String, Object> containerData = new HashMap<>();
				containerData.put(TransformationCatalogKeywords.NAME.getReservedName(), container.getName());
				containerData.put(TransformationCatalogKeywords.CONTAINER_IMAGE_SITE.getReservedName(),
						container.getImageSite());
				if (container.getImageDefinitionURL() != null) {
					containerData.put(TransformationCatalogKeywords.CONTAINER_DOCKERFILE.getReservedName(),
							container.getImageDefinitionURL());
				}
				containerData.put(TransformationCatalogKeywords.CONTAINER_IMAGE.getReservedName(),
						container.getImageURL().getURL());
				containerData.put(TransformationCatalogKeywords.CONTAINER_MOUNT.getReservedName(),
						container.getMountPoints());
				containerData.put(TransformationCatalogKeywords.TYPE.getReservedName(), container.getType());
				List<Profile> profiles = container.getProfiles();
				if (profiles != null) {
					Object profileData = buildProfiles(profiles, false);
					if (profileData != null) {
						containerData.put(TransformationCatalogKeywords.PROFILE.getReservedName(),
								buildProfiles(profiles, false));
					}
					Object metaData = buildProfiles(profiles, true);
					if (metaData != null) {
						containerData.put(TransformationCatalogKeywords.METADATA.getReservedName(), metaData);
					}
				}
				containers.add(containerData);
			}
			transformationData.add(transformationMap);
			transformationData.add(containerMap);
			mapper.writeValue(out, transformationData);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	private static Map<String, Object> getEntryMap(List<Map<String, Object>> transformations, String logicalName) {
		for (Map<String, Object> transformation : transformations) {
			if (transformation.get(TransformationCatalogKeywords.NAME.getReservedName()).equals(logicalName)) {
				transformations.remove(transformation);
				return transformation;
			}
		}
		return new LinkedHashMap<String, Object>();
	}

	private static Map<String, Object> buildSite(String resourceId, String pfn, Container container, SysInfo sysInfo,
			TCType type) {
		Map<String, Object> siteInfo = new LinkedHashMap<>();
		siteInfo.put(TransformationCatalogKeywords.NAME.getReservedName(), resourceId);
		siteInfo.put(TransformationCatalogKeywords.SITE_ARCHITECTURE.getReservedName(), sysInfo.getArchitecture());
		siteInfo.put(TransformationCatalogKeywords.SITE_OS.getReservedName(), sysInfo.getOS());
		if(container != null) {
			siteInfo.put(TransformationCatalogKeywords.SITE_CONTAINER_NAME.getReservedName(), container.getName());
		}
		siteInfo.put(TransformationCatalogKeywords.SITE_OS_RELEASE.getReservedName(), sysInfo.getOSRelease());
		siteInfo.put(TransformationCatalogKeywords.SITE_OS_VERSION.getReservedName(), sysInfo.getOSVersion());
		siteInfo.put(TransformationCatalogKeywords.SITE_PFN.getReservedName(), pfn);
		siteInfo.put(TransformationCatalogKeywords.TYPE.getReservedName(), type);
		return siteInfo;
	}

	private static Object buildProfiles(List<Profile> profiles, boolean isMeta) {
		List<Map<String, Map<String, Object>>> profileList = null;
		for (Profile profile : profiles) {
			String nameSpace = profile.getProfileNamespace();
			if (isMeta) {
				if (nameSpace.contains("meta")) {
					profileList = new LinkedList<Map<String, Map<String, Object>>>();
					String key = profile.getProfileKey();
					String value = profile.getProfileValue();
					getMap(nameSpace, profileList).put(key, value);
				}
			} else {
				if (!nameSpace.contains("meta")) {
					profileList = new LinkedList<Map<String, Map<String, Object>>>();
					String key = profile.getProfileKey();
					String value = profile.getProfileValue();
					getMap(nameSpace, profileList).put(key, value);
				}
			}
		}
		return profileList;

	}

	private static Map<String, Object> getMap(String nameSpace, List<Map<String, Map<String, Object>>> profileList) {
		if (profileList.isEmpty()) {
			Map<String, Object> keyValueMap = new HashMap<>();
			Map<String, Map<String, Object>> maps = new HashMap<>();
			maps.put(nameSpace, keyValueMap);
			profileList.add(maps);
			return keyValueMap;
		} else {
			for (Map<String, Map<String, Object>> maps : profileList) {
				if (maps.containsKey(nameSpace)) {
					return maps.get(nameSpace);
				} else {
					Map<String, Object> keyValueMap = new HashMap<>();
					maps.put(nameSpace, keyValueMap);
					profileList.add(maps);
					return keyValueMap;
				}
			}
		}
		return null;

	}

}
