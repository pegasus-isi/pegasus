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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.snakeyaml.parser.ParserException;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.transformation.TransformationCatalogEntry;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container;
import edu.isi.pegasus.planner.catalog.transformation.classes.Container.TYPE;
import edu.isi.pegasus.planner.catalog.transformation.impl.Abstract;
import edu.isi.pegasus.planner.catalog.transformation.classes.TCType;
import edu.isi.pegasus.planner.catalog.transformation.classes.TransformationStore;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.parser.tokens.TransformationCatalogKeywords;

/**
 * Parses the input stream and generates the TransformationStore as output.
 *
 * This parser is able to parse the Transformation Catalog specification in the
 * following format
 * 
 * <pre>
	- namespace: "ls"
	  name: "keg"    
	  version: 1.0
	
	  profile:
	   - environment: 
	      "APP_HOME": "/tmp/myscratch"
	      "JAVA_HOME": "/opt/java/1.6"
	
	  site:
	   - name: "isi"
	     profile: 
	      environment: 
	       "HELLo": "WORLD"
	       "JAVA_HOME": "/opt/java/1.6"
	       condor: 
	        "FOO": "bar"
	     pfn: /usr/bin/ls
	     arch: x86
	     osrelease: fc
	     osversion: 4
	     os_type: INSTALLED
	      
	   - name: "ads"
	     profile: 
	      environment: 
	       "HELLo": "WORLD"
	       "JAVA_HOME": "/opt/java/1.6"
	       condor: 
	        "FOO": "bar"
	     pfn: /path/to/keg
	     arch: x86
	     os: linux
	     osrelease: fc
	     osversion: 4
	     os_type: INSTALLED
	     container: "centos-pegasus"
	     
	  cont:
	   - name: "centos-pegasus"
	     image: docker:///rynge/montage:latest
	     image_site: optional site
	     mount: /Volumes/Work/lfs1:/shared-data/:ro
	     profile: 
	      environment: 
	       "JAVA_HOME": "/opt/java/1.6"
	
	- namespace: "cat"
	  name: "keg"
	  version: 1.0
	
	  site:    
	   - name: "ads"
	     profile: 
	      environment: 
	       "HELLo": "WORLD"
	       "JAVA_HOME": "/opt/java/1.6"
	       condor: 
	        "FOO": "bar"
	     pfn: /usr/bin/cat
	     arch: x86
	     os: linux
	     osrelease: fc
	     osversion: 4
	     os_type: INSTALLED
 * </pre>
 *
 * @author Mukund Murrali
 * @version $Revision$
 *
 *
 */
public class TransformationCatalogYAMLParser {

	/**
	 * Schema file name;
	 **/
	private static final String SCHEMA_FILENAME = "/home/mukund/temp/pegasus_mybranch/share/pegasus/schema/transformationcatalog.json";

	//private static final String SCHEMA_FILENAME = "http://pegasus.isi.edu/schema/transformationcatalog.json";
	/**
	 * Schema File Object;
	 **/
	private static final File YAMLSCHEMA = new File(SCHEMA_FILENAME);

	/**
	 * The transformation to the logger used to log messages.
	 */
	private LogManager mLogger;

	/**
	 * This reader is used for reading the contents of the YAML file
	 **/
	private InputStream mStream;

	/**
	 * Initializes the parser with an input stream to read from.
	 *
	 * @param input  is the stream opened for reading.
	 * @param logger the transformation to the logger.
	 *
	 * @throws IOException
	 * @throws ScannerException
	 */
	public TransformationCatalogYAMLParser(InputStream stream, LogManager logger) throws IOException, ScannerException {
		mStream = stream;
		mLogger = logger;
	}

	/**
	 * Parses the complete input stream
	 *
	 * @param modifyFileURL Boolean indicating whether to modify the file URL or not
	 *
	 * @return TransformationStore
	 *
	 * @throws ScannerException
	 * @throws 
	 */
	@SuppressWarnings("unchecked")
	public TransformationStore parse(boolean modifyFileURL) throws ScannerException {

		TransformationStore store = new TransformationStore();

		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

		Object yamlData = null;

		/**
		 * Loads the yaml data
		 * **/
		try {

			yamlData = mapper.readValue(mStream, Object.class);
		} catch (ParserException e) {
			String errorMessage = parseError(e);
			throw new ScannerException(e.getProblemMark().getLine() + 1, errorMessage);
		} catch (Exception e) {
			throw new ScannerException("Error in loading the yaml file", e);
		}

		YAMLSchemaValidationResult result = YAMLSchemaValidator.getInstance().validateYAMLSchema(yamlData, YAMLSCHEMA);

		//schema validation is done here.. in case of any validation error we throw the result..
		if (!result.isSuccess()) {
			List<String> errors = result.getErrorMessage();
			StringBuilder errorResult = new StringBuilder();
			int i = 1;
			for (String error : errors) {
				if (i > 1) {
					errorResult.append(",");
				}
				errorResult.append("Error ").append(i++).append(":{");
				errorResult.append(error).append("}");
			}
			throw new ScannerException(errorResult.toString());
		}

		List<Object> transformationData = (List<Object>) yamlData;

		for (Object transformation : transformationData) {

			Map<String, Object> transformationAndContainers = (Map<String, Object>) transformation;

			/**
			 * Based on containers/transformations the corresponding data is loaded..
			 **/
			if (transformationAndContainers.containsKey(TransformationCatalogKeywords.CONTAINER.getReservedName())) {
				List<Object> containerInformations = (List<Object>) transformationAndContainers
						.get(TransformationCatalogKeywords.CONTAINER.getReservedName());
				for (Object containerInformationObjects : containerInformations) {

					Map<String, Object> containerInformation = (Map<String, Object>) containerInformationObjects;

					Container container = new Container();

					getContainerInfo(container, containerInformation);

					// we have information about one transformation catalog container
					mLogger.log("Container Entry parsed is - " + container, LogManager.DEBUG_MESSAGE_LEVEL);

					store.addContainer(container);
				}

			} else {
				List<Object> transformationInformations = (List<Object>) transformationAndContainers
						.get(TransformationCatalogKeywords.TRANSFORMATION.getReservedName());

				for (Object transformationInfo : transformationInformations) {

					Map<String, Object> singleTransformation = (Map<String, Object>) transformationInfo;

					/**
					 * Get the basic properties for the transformation..
					 * **/
					String nameSpace = (String) singleTransformation
							.get(TransformationCatalogKeywords.NAMESPACE.getReservedName());

					String name = (String) singleTransformation
							.get(TransformationCatalogKeywords.NAME.getReservedName());

					Double version_obj = (Double) singleTransformation
							.get(TransformationCatalogKeywords.VERSION.getReservedName());
					
					String version = null;
					
					if(version_obj != null) {
						version = String.valueOf(version_obj);
					}

					Object profileObj = singleTransformation
							.get(TransformationCatalogKeywords.PROFILE.getReservedName());

					Object metaObj = singleTransformation.get(TransformationCatalogKeywords.METADATA.getReservedName());

					Profiles profiles = getProfilesForTransformation(profileObj, metaObj);

					List<Object> sites = (List<Object>) singleTransformation
							.get(TransformationCatalogKeywords.SITE.getReservedName());

					for (Object siteObj : sites) {

						Map<String, Object> siteData = (Map<String, Object>) siteObj;

						TransformationCatalogEntry entry = new TransformationCatalogEntry(nameSpace, name, version);

						getTransformationCatalogEntry(entry, siteData, profiles);

						if (modifyFileURL) {
							store.addEntry(Abstract.modifyForFileURLS(entry));
						} else {
							store.addEntry(entry);
						}

						// we have information about one transformation catalog container
						mLogger.log("Transformation Catalog Entry parsed is - " + entry,
								LogManager.DEBUG_MESSAGE_LEVEL);
					}
				}
			}
		}
		store.resolveContainerReferences();
		return store;
	}

	/**
	 * This method is used to extract the necessary information from the parsing exception
	 * 
	 * @param e The parsing exception generated from the yaml.
	 * 
	 * @return String representing the line number and the problem is returned
	 */
	private String parseError(ParserException e) {
		StringBuilder builder = new StringBuilder();
		builder.append("Problem in the line :" + (e.getProblemMark().getLine() + 1) + ", column:"
				+ e.getProblemMark().getColumn() + " with tag "
				+ e.getProblemMark().get_snippet().replaceAll("\\s", ""));
		return builder.toString();
	}

	/**
	 * This method is to load all the container information from the yaml data..
	 * 
	 * @param container - The container object to be populated. 
	 * @param containerInformation - Map representing the container related information from the yaml data
	 * 
	 * 
	 */
	private void getContainerInfo(Container container, Map<String, Object> containerInformation) {

		for (Entry<String, Object> entries : containerInformation.entrySet()) {

			String key = entries.getKey();

			TransformationCatalogKeywords reservedKey = TransformationCatalogKeywords.getReservedKey(key);

			if (reservedKey == null) {
				throw new ScannerException(-1, "Illegeal key " + key + " for container ");
			}

			switch (reservedKey) {
			case NAME:
				String containerName = (String) containerInformation.get(key);
				container.setName(containerName);
				break;

			case TYPE:
				String type = (String) containerInformation.get(key);
				container.setType(TYPE.valueOf(type));
				break;

			case CONTAINER_IMAGE:
				String url = (String) containerInformation.get(key);
				container.setImageURL(url);
				break;

			case CONTAINER_IMAGE_SITE:
				String imageSite = (String) containerInformation.get(key);
				container.setImageSite(imageSite);
				break;

			case CONTAINER_DOCKERFILE:
				String dockerFile = (String) containerInformation.get(key);
				container.setImageDefinitionURL(dockerFile);
				break;

			case CONTAINER_MOUNT:
				String mountPoint = (String) containerInformation.get(key);
				container.addMountPoint(mountPoint);
				break;

			case PROFILE:
				Object profileObj = containerInformation.get(key);
				Profiles profiles = getProfilesForTransformation(profileObj, null);
				container.addProfiles(profiles);

			default:
				break;
			}

		}

	}

	/**
	 * 
	 * This function is used to populate the site related information from the yaml object
	 * 
	 * @param entry - Entry to populate the site data
	 * @param siteData - Map representing the site information
	 * @param profiles - Global profiles of a transformation. This is for overwritting the site data
	 * 
	 */
	private void getTransformationCatalogEntry(TransformationCatalogEntry entry, Map<String, Object> siteData,
			Profiles profiles) {

		SysInfo systemInfo = new SysInfo();

		Object profileObj = null, metaObj = null;

		for (Entry<String, Object> entries : siteData.entrySet()) {
			String key = entries.getKey();

			TransformationCatalogKeywords reservedKey = TransformationCatalogKeywords.getReservedKey(key);

			if (reservedKey == null) {
				throw new ScannerException(-1, "Illegeal key " + key + " for container ");
			}

			switch (reservedKey) {

			case NAME:
				String siteName = (String) siteData.get(key);
				entry.setResourceId(siteName);
				break;

			case SITE_ARCHITECTURE:
				String architecture = (String) siteData.get(key);
				systemInfo.setArchitecture(SysInfo.Architecture.valueOf(architecture));
				break;

			case SITE_OS:
				String os = (String) siteData.get(key);
				systemInfo.setOS(SysInfo.OS.valueOf(os));
				break;

			case SITE_OS_RELEASE:
				String release = (String) siteData.get(key);
				systemInfo.setOSRelease(release);
				break;

			case SITE_OS_VERSION:
				Integer osVersion = (Integer) siteData.get(key);
				systemInfo.setOSVersion(String.valueOf(osVersion));
				break;

			case TYPE:
				String type = (String) siteData.get(key);
				entry.setType(TCType.valueOf(type.toUpperCase()));
				break;

			case PROFILE:
				profileObj = siteData.get(key);
				break;

			case METADATA:
				metaObj = siteData.get(key);
				break;

			case SITE_PFN:
				String pfn = (String) siteData.get(key);
				entry.setPhysicalTransformation(pfn);
				break;

			case SITE_CONTAINER_NAME:
				String containerName = (String) siteData
						.get(TransformationCatalogKeywords.SITE_CONTAINER_NAME.getReservedName());

				entry.setContainer(new Container(containerName));
				break;

			default:
				break;
			}

		}
		Profiles siteProfiles = getProfilesForTransformation(profileObj, metaObj);

		for (Profile profile : siteProfiles.getProfiles()) {
			profiles.addProfileDirectly(profile);
		}

		// add all the profiles for the container only if they are empty
		if (!profiles.isEmpty()) {
			entry.addProfiles(profiles);
		}

		entry.setSysInfo(systemInfo);
	}

	/**
	 * Remove potential leading and trainling quotes from a string.
	 *
	 * @param input is a string which may have leading and trailing quotes
	 * @return a string that is either identical to the input, or a substring
	 *         thereof.
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
		} else {
			return input;
		}
	}

	/**
	 * Returns a list of profiles that have to be applied to the entries for all the
	 * sites corresponding to a transformation.
	 * 
	 * @param metaObj
	 * @param profileObj
	 *
	 * @return Profiles specified
	 *
	 * @throws IOException
	 * @throws ScannerException
	 */
	@SuppressWarnings("unchecked")
	private Profiles getProfilesForTransformation(Object profileObj, Object metaObj) {
		Profiles profiles = new Profiles();
		if (profileObj != null) {
			List<Object> profileInfo = (List<Object>) profileObj;
			for (Object profile : profileInfo) {
				Map<String, Object> profileMaps = (Map<String, Object>) profile;
				for (Entry<String, Object> profileMapsEntries : profileMaps.entrySet()) {
					String profileName = profileMapsEntries.getKey();
					if (Profile.namespaceValid(profileName)) {
						Map<String, String> profileMap = (Map<String, String>) profileMapsEntries.getValue();
						for (Entry<String, String> profileMapEntries : profileMap.entrySet()) {
							profiles.addProfile(new Profile(profileName, niceString(profileMapEntries.getKey()),
									niceString(profileMapEntries.getValue())));
						}
					}
				}
			}
		}
		if (metaObj != null) {
			Map<String, String> metaMap = (Map<String, String>) metaObj;
			for (Entry<String, String> profileMapEntries : metaMap.entrySet()) {
				profiles.addProfile(new Profile(Profile.METADATA, niceString(profileMapEntries.getKey()),
						niceString(profileMapEntries.getValue())));
			}
		}
		return profiles;
	}

	/**
	 * Test function.
	 *
	 * @param args
	 * @throws ProcessingException
	 */
	public static void main(String[] args) throws ScannerException {
		try {
			InputStream r = new FileInputStream(
					new File("/home/mukund/workspace/YAMLTesting/transformationcatalogue_v2.yaml"));

			LogManager logger = LogManagerFactory.loadSingletonInstance();
			logger.setLevel(LogManager.DEBUG_MESSAGE_LEVEL);
			logger.logEventStart("event.pegasus.catalog.transformation.test", "planner.version", "2");

			TransformationCatalogYAMLParser p = new TransformationCatalogYAMLParser(r, logger);
			p.parse(true);

		} catch (FileNotFoundException ex) {
			Logger.getLogger(TransformationCatalogYAMLParser.class.getName()).log(Level.SEVERE, null, ex);
		} catch (ScannerException se) {
			se.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}

	}
}
