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

package edu.isi.pegasus.planner.parser;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
//import com.fasterxml.jackson.dataformat.yaml.snakeyaml.parser.ParserException;
import com.fasterxml.jackson.dataformat.yaml.JacksonYAMLParseException;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.common.util.PegasusURL;
import edu.isi.pegasus.planner.catalog.classes.Profiles;
import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import edu.isi.pegasus.planner.catalog.classes.SysInfo.Architecture;
import edu.isi.pegasus.planner.catalog.classes.SysInfo.OS;
import edu.isi.pegasus.planner.catalog.site.classes.Connection;
import edu.isi.pegasus.planner.catalog.site.classes.Directory;
import edu.isi.pegasus.planner.catalog.site.classes.Directory.TYPE;
import edu.isi.pegasus.planner.catalog.site.classes.FileServer;
import edu.isi.pegasus.planner.catalog.site.classes.FileServerType.OPERATION;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway.JOB_TYPE;
import edu.isi.pegasus.planner.catalog.site.classes.GridGateway.SCHEDULER_TYPE;
import edu.isi.pegasus.planner.catalog.site.classes.InternalMountPoint;
import edu.isi.pegasus.planner.catalog.site.classes.ReplicaCatalog;
import edu.isi.pegasus.planner.catalog.site.classes.SiteCatalogEntry;
import edu.isi.pegasus.planner.catalog.site.classes.SiteDataVisitor;
import edu.isi.pegasus.planner.catalog.site.classes.SiteStore;
import edu.isi.pegasus.planner.catalog.site.classes.XML4PrintVisitor;
import edu.isi.pegasus.planner.classes.PegasusBag;
import edu.isi.pegasus.planner.classes.Profile;
import edu.isi.pegasus.planner.common.PegasusProperties;
import edu.isi.pegasus.planner.common.VariableExpansionReader;

/**
 * This class uses the Xerces SAX2 parser to validate and parse an XML document
 * conforming to the Site Catalog schema v4.0
 * 
 * 
 * http://pegasus.isi.edu/schema/sc-3.0.xsd
 * 
 * @author Mukund Murrali
 * @version $Revision$
 */
public class SiteCatalogYAMLParser {

	/**
	 * The "not-so-official" location URL of the Site Catalog Schema.
	 */
	public static final String SCHEMA_LOCATION = "http://pegasus.isi.edu/schema/sc-5.0.json";

	/**
	 * Schema file name
	 **/
	public static final String SCHEMA_URI = "sc-5.0.json";

	/**
	 * The final result constructed.
	 */
	private SiteStore mResult;

	/**
	 * The set of sites that need to be parsed.
	 */
	private Set<String> mSites;

	/**
	 * A boolean indicating whether to load all sites.
	 */
	private boolean mLoadAll;

	/**
	 * flag to denote if the parsing is done or not
	 **/
	private boolean mParsingDone = false;

	/**
	 * Logger for logging the properties..
	 **/
	private final LogManager mLogger;

	/**
	 * Holder for various Pegasus properties..
	 * 
	 */
	private final PegasusProperties mProps;

	/**
	 * File object of the schema..
	 **/

	private final File SCHEMA_FILENAME;

	/**
	 * The constructor.
	 *
	 * @param bag   the bag of initialization objects.
	 * @param sites the list of sites that need to be parsed. * means all.
	 */
	public SiteCatalogYAMLParser(PegasusBag bag, List<String> sites) {
		mSites = new HashSet<String>();
		for (Iterator<String> it = sites.iterator(); it.hasNext();) {
			mSites.add(it.next());
		}
		mLoadAll = mSites.contains("*");
		mLogger = bag.getLogger();
		mProps = bag.getPegasusProperties();
		SCHEMA_FILENAME = new File(mProps.getSchemaDir(), new File(SCHEMA_URI).getName());
	}

	/**
	 * Returns the constructed site store object
	 * 
	 * @return <code>SiteStore<code> if parsing completed
	 */
	public SiteStore getSiteStore() {
		if (mParsingDone) {
			return mResult;
		} else {
			throw new RuntimeException("Parsing of file needs to complete before function can be called");
		}
	}

	/**
	 * The main method that starts the parsing.
	 * 
	 * @param file the YAML file to be parsed.
	 */
	@SuppressWarnings("unchecked")
	public void startParser(String file) {
		try {

			File f = new File(file);

			if (!(f.exists() && f.length() > 0)) {
				mResult = new SiteStore();
				mLogger.log("The Site Catalog file " + file + " was not found or empty", LogManager.INFO_MESSAGE_LEVEL);
				mParsingDone = true;
				return;
			}
			Reader mReader = new VariableExpansionReader(new FileReader(f));

			ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

			Object yamlData = null;

			/**
			 * Loads the yaml data
			 **/
			try {
				yamlData = mapper.readValue(mReader, Object.class);
			} catch (JacksonYAMLParseException e) {
				String errorMessage = parseError(e);
				//throw new ScannerException(e.getProblemMark().getLine() + 1, errorMessage);
                                throw new ScannerException(errorMessage);
			} catch (Exception e) {
				throw new ScannerException("Error in loading the yaml file", e);
			}
			if (yamlData != null) {
				YAMLSchemaValidationResult result = YAMLSchemaValidator.getInstance().validate(yamlData,
						SCHEMA_FILENAME, "site");

				// schema validation is done here.. in case of any validation error we throw the
				// result..
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
				mResult = new SiteStore();

				//the yaml data we store is in the form of map structure..
				Map<String, Object> data = (LinkedHashMap<String, Object>) yamlData;
				
				//the "site" top level will have list of site catalog data which we need to populate..
				List<Object> siteCatatalogs = (List<Object>) data.get("site");
				for (Object siteCatalog : siteCatatalogs) {
					//each of the linked list object has a map of key, value representing the data.
					Map<String, Object> siteCatalogInfo = (LinkedHashMap<String, Object>) siteCatalog;
					SiteCatalogEntry entry = new SiteCatalogEntry();
					
					/***
					 * Here we construct the top level information for a site.
					 */
					Object architecture = siteCatalogInfo.get("arch");
					if (architecture != null) {
						entry.setArchitecture(Architecture.valueOf((String) architecture));
					}

					Object os = siteCatalogInfo.get("os");
					if (os != null) {
						entry.setOS(OS.valueOf((String) os));
					}

					Object handle = siteCatalogInfo.get("handle");
					if (handle != null) {
						entry.setSiteHandle((String) handle);
					}

					Object osrelease = siteCatalogInfo.get("osrelease");
					if (osrelease != null) {
						entry.setOSRelease(((String) osrelease));
					}

					Object osversion = siteCatalogInfo.get("osversion");
					if (osversion != null) {
						entry.setOSVersion(((String) osversion));
					}

					Object glibc = siteCatalogInfo.get("glibc");
					if (glibc != null) {
						entry.setGlibc((String) glibc);
					}

					Object profileObj = siteCatalogInfo.get("profile");

					Object metaObj = siteCatalogInfo.get("metadata");

					//construction of profiles and metadata if present..
					List<Profile> profiles = getProfilesForTransformation(profileObj, metaObj).getProfiles();

					for (Profile profile : profiles) {
						entry.addProfile(profile);
					}

					Object directoryObject = siteCatalogInfo.get("directory");

					//if directory is present in the site, we extract those and parse it..
					if (directoryObject != null) {
						List<Object> directories = (List<Object>) directoryObject;
						for (Object directory : directories) {
							Directory d = new Directory();
							InternalMountPoint mountPoint = new InternalMountPoint();
							d.setInternalMountPoint(mountPoint);

							Map<String, Object> directoryInfo = (Map<String, Object>) directory;

							Object type = directoryInfo.get("type");
							if (type != null) {
								d.setType((String) type);
							}

							Object path = directoryInfo.get("path");
							if (path != null) {
								mountPoint.setMountPoint((String) path);
							}

							Object free_size = directoryInfo.get("free-size");
							if (free_size != null) {
								mountPoint.setFreeSize((String) free_size);
							}

							Object total_size = directoryInfo.get("total-size");
							if (total_size != null) {
								mountPoint.setTotalSize((String) total_size);
							}

							List<Object> fileserversInfo = (List<Object>) directoryInfo.get("file-server");
							List<FileServer> fileservers = new LinkedList<FileServer>();

							for (Object fileserverInfo : fileserversInfo) {
								FileServer fs = new FileServer();
								Map<String, Object> fileServerMap = (Map<String, Object>) fileserverInfo;

								Object protocol = fileServerMap.get("protocol");
								if (protocol != null) {
									fs.setProtocol((String) protocol);
								}

								Object mount_point = fileServerMap.get("mount-point");
								if (mount_point != null) {
									fs.setMountPoint((String) mount_point);
								}

								Object operation = fileServerMap.get("operation");
								if (operation != null) {
									fs.setSupportedOperation(OPERATION.valueOf((String) operation));
								}

								Object url = fileServerMap.get("url");
								if (url != null) {
									PegasusURL pegasusurl = new PegasusURL((String) url);
									fs.setURLPrefix(pegasusurl.getURLPrefix());
									fs.setProtocol(pegasusurl.getProtocol());
									fs.setMountPoint(pegasusurl.getPath());
								}

								profileObj = siteCatalogInfo.get("profile");

								metaObj = siteCatalogInfo.get("metadata");

								profiles = getProfilesForTransformation(profileObj, metaObj).getProfiles();

								for (Profile profile : profiles) {
									fs.addProfile(profile);
								}

								fileservers.add(fs);
							}

							d.setFileServers(fileservers);
							entry.addDirectory(d);
						}
					}

					//if grid  is present in the site, we extract those and parse it..
					Object gridObject = siteCatalogInfo.get("grid");

					if (gridObject != null) {
						List<Object> gridsInformation = (List<Object>) gridObject;
						for (Object gridInfo : gridsInformation) {
							GridGateway gw = new GridGateway();
							Map<String, Object> gridInformationMap = (Map<String, Object>) gridInfo;

							Object arch = gridInformationMap.get("arch");
							if (arch != null) {
								gw.setArchitecture(Architecture.valueOf((String) arch));
							}

							Object type = gridInformationMap.get("type");
							if (type != null) {
								gw.setType(GridGateway.TYPE.valueOf((String) type));
							}

							Object contact = gridInformationMap.get("contact");
							if (contact != null) {
								gw.setContact((String) contact);
							}

							Object scheduler = gridInformationMap.get("scheduler");
							if (scheduler != null) {
								gw.setScheduler((String) scheduler);
							}

							Object jobtype = gridInformationMap.get("jobtype");
							if (jobtype != null) {
								gw.setJobType(GridGateway.JOB_TYPE.valueOf((String) jobtype));
							}

							os = gridInformationMap.get("os");
							if (os != null) {
								gw.setOS(SysInfo.OS.valueOf(((String) os).toLowerCase()));
							}

							osrelease = gridInformationMap.get("osrelease");
							if (osrelease != null) {
								gw.setOSRelease(((String) osrelease));
							}

							osversion = gridInformationMap.get("osversion");
							if (osversion != null) {
								gw.setOSVersion(((String) osversion));
							}

							glibc = gridInformationMap.get("glibc");
							if (glibc != null) {
								gw.setGlibc((String) glibc);
							}

							Object idle_nodes = gridInformationMap.get("idle-nodes");
							if (idle_nodes != null) {
								gw.setIdleNodes((String) idle_nodes);
							}

							Object total_nodes = gridInformationMap.get("total-nodes");
							if (total_nodes != null) {
								gw.setTotalNodes((String) total_nodes);
							}

							entry.addGridGateway(gw);
						}
					}

					//if replica is present in the site, we extract those and parse it..
					Object replicaCatalogObject = siteCatalogInfo.get("replica-catalog");

					if (replicaCatalogObject != null) {
						List<Object> replicaCatalogs = (List<Object>) replicaCatalogObject;
						for (Object replicationCatalog : replicaCatalogs) {
							ReplicaCatalog rc = new ReplicaCatalog();
							Map<String, Object> replicationCatalogInfo = (Map<String, Object>) replicationCatalog;

							Object type = replicationCatalogInfo.get("type");
							if (type != null) {
								rc.setType((String) type);
							}

							Object url = replicationCatalogInfo.get("url");
							if (url != null) {
								rc.setURL((String) url);
							}

							Object aliasObj = replicationCatalogInfo.get("alias");

							if (aliasObj != null) {
								List<String> aliases = (List<String>) aliasObj;
								for (String alias : aliases) {
									rc.addAlias(alias);
								}
							}

							Object connectionObj = replicationCatalogInfo.get("connection");

							if (connectionObj != null) {
								List<Object> connections = (List<Object>) connectionObj;
								for (Object connection : connections) {
									Map<String, String> connectionMap = (Map<String, String>) connection;
									Connection c = new Connection();
									c.setKey(connectionMap.get("key"));
									c.setValue(connectionMap.get("value"));
									rc.addConnection(c);
								}
							}
							entry.addReplicaCatalog(rc);
						}
					}
					if (loadSite(entry)) {
						mResult.addEntry(entry);
					}
				}
			}

		} catch (IOException ioe) {
			mLogger.log("IO Error :" + ioe.getMessage(), LogManager.ERROR_MESSAGE_LEVEL);
		}
		mParsingDone = true;
	}

	/**
	 * Whether to laod a site or not in the <code>SiteStore</code>
	 * 
	 * @param site the <code>SiteCatalogEntry</code> object.
	 * 
	 * @return boolean
	 */
	private boolean loadSite(SiteCatalogEntry site) {
		return (mLoadAll || mSites.contains(site.getSiteHandle()));
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
							Object key = profileMapEntries.getKey();
							Object value = profileMapEntries.getValue();
							profiles.addProfile(new Profile(profileName, niceString(String.valueOf(key)),
									niceString(String.valueOf(value))));
						}
					}
				}
			}
		}
		if (metaObj != null) {
			Map<String, String> metaMap = (Map<String, String>) metaObj;
			for (Entry<String, String> profileMapEntries : metaMap.entrySet()) {
				Object key = profileMapEntries.getKey();
				Object value = profileMapEntries.getValue();
				profiles.addProfile(new Profile(Profile.METADATA, niceString(String.valueOf(key)),
						niceString(String.valueOf(value))));
			}
		}
		return profiles;
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
	 * This method is used to extract the necessary information from the parsing
	 * exception
	 * 
	 * @param e The parsing exception generated from the yaml.
	 * 
	 * @return String representing the line number and the problem is returned
	 */
	private String parseError(JacksonYAMLParseException e) {
            /*
		StringBuilder builder = new StringBuilder();
		builder.append("Problem in the line :" + (e.getProblemMark().getLine() + 1) + ", column:"
				+ e.getProblemMark().getColumn() + " with tag "
				+ e.getProblemMark().get_snippet().replaceAll("\\s", ""));
		return builder.toString();
            */
            return e.toString();
	}

	/**
	 * This code is to parse any format to YAML..
	 * Here we iterate the store results and convert it to the map format
	 * and finally write to the specified file..
	 * **/
	public static void parseToYAML(SiteStore result, String mOutputFile) {
		ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
		try {
			Writer out = new BufferedWriter(new FileWriter(mOutputFile));
			Map<String, Object> siteData = new LinkedHashMap<String, Object>();
			List<Object> siteCatalogs = new LinkedList<Object>();
			Iterator<SiteCatalogEntry> siteIterator = result.entryIterator();
			if (siteIterator.hasNext()) {
				siteData.put("site", siteCatalogs);
			}
			while (siteIterator.hasNext()) {

				SiteCatalogEntry entry = siteIterator.next();

				Map<String, Object> siteCatalogInfo = new LinkedHashMap<String, Object>();

				Architecture architecture = entry.getArchitecture();


				String handle = entry.getSiteHandle();
				if (handle != null) {
					siteCatalogInfo.put("handle", handle);
				}
				
				if (architecture != null) {
					siteCatalogInfo.put("arch", architecture.name());
				}

				OS os = entry.getOS();
				if (os != null) {
					siteCatalogInfo.put("os", os.name());
				}	

				String osrelease = entry.getOSRelease();
				if (osrelease != null) {
					siteCatalogInfo.put("osrelease", osrelease);
				}

				String osversion = entry.getOSVersion();
				siteCatalogInfo.get("osversion");
				if (osversion != null) {
					siteCatalogInfo.put("osversion", osversion);
				}

				String glibc = entry.getGlibc();
				if (glibc != null) {
					siteCatalogInfo.put("glibc", glibc);
				}

				Iterator<Directory> directoryIter = entry.getDirectoryIterator();

				List<Object> directories = new LinkedList<>();

				if (directoryIter.hasNext()) {
					siteCatalogInfo.put("directory", directories);
				}

				while (directoryIter.hasNext()) {

					Directory directoryEntry = directoryIter.next();

					Map<String, Object> directoryInfo = new LinkedHashMap<String, Object>();

					TYPE type = directoryEntry.getType();
					if (type != null) {
						directoryInfo.put("type", type.name());
					}

					InternalMountPoint mountPoint = directoryEntry.getInternalMountPoint();
					if (mountPoint != null) {
						String path = mountPoint.getMountPoint();
						if (path != null) {
							directoryInfo.put("path", path);
						}

						String free_size = mountPoint.getFreeSize();
						if (free_size != null && free_size != "") {
							directoryInfo.put("free-size", free_size);
						}

						String total_size = mountPoint.getTotalSize();
						if (total_size != null && total_size != "") {
							directoryInfo.put("total-size", total_size);
						}
					}
					List<FileServer> fileserversInfo = new LinkedList<>();
					for (FileServer.OPERATION operation : FileServer.OPERATION.values()) {
						List<FileServer> fileserver = directoryEntry.getFileServers(operation);
						if (fileserver != null) {
							fileserversInfo.addAll(fileserver);
						}
					}
					if (!fileserversInfo.isEmpty()) {
						List<Object> fileservers = new LinkedList<Object>();
						directoryInfo.put("file-server", fileservers);

						for (FileServer fileserverInfo : fileserversInfo) {
							Map<String, Object> fileServerMap = new LinkedHashMap<String, Object>();

							String protocol = fileserverInfo.getProtocol();
							if (protocol != null) {
								fileServerMap.put("protocol", protocol);
							}

							String urlPrefix = fileserverInfo.getURLPrefix();
							if (urlPrefix != null) {
								StringBuilder url = new StringBuilder(urlPrefix);
								url.append(fileserverInfo.getMountPoint());
								fileServerMap.put("url", url.toString());
							}

							String mount_point = fileserverInfo.getMountPoint();
							if (mount_point != null) {
								fileServerMap.put("mount-point", mount_point);
							}

							FileServer.OPERATION operation = fileserverInfo.getSupportedOperation();
							if (operation != null) {
								fileServerMap.put("operation", operation);
							}

							if (fileserverInfo.getProfiles() != null) {
								List<Map<String, Map<String, Object>>> profileData = buildProfiles(
										fileserverInfo.getProfiles().getProfiles());
								if (profileData != null && profileData.size() > 0) {
									fileServerMap.put("profile", profileData);
								}
								Map<String, Object> metaData = buildMeta(fileserverInfo.getProfiles().getProfiles());
								if (metaData != null && metaData.size() > 0) {
									fileServerMap.put("metadata", metaData);
								}
							}
							fileservers.add(fileServerMap);
						}
					}
					directories.add(directoryInfo);
				}

				Iterator<GridGateway> gridGateWayIter = entry.getGridGatewayIterator();

				List<Object> gateways = new LinkedList<>();

				if (gridGateWayIter.hasNext()) {
					siteCatalogInfo.put("grid", gateways);
				}

				while (gridGateWayIter.hasNext()) {
					Map<String, Object> gripInfoMap = new LinkedHashMap<String, Object>();

					GridGateway gatewayentry = gridGateWayIter.next();

					Architecture arch = gatewayentry.getArchitecture();
					if (arch != null) {
						gripInfoMap.put("arch", arch.name());
					}

					edu.isi.pegasus.planner.catalog.site.classes.GridGateway.TYPE type = gatewayentry.getType();
					if (type != null) {
						gripInfoMap.put("type", type.name());
					}

					String contact = gatewayentry.getContact();
					if (contact != null) {
						gripInfoMap.put("contact", contact);
					}

					SCHEDULER_TYPE scheduler = gatewayentry.getScheduler();
					if (scheduler != null) {
						gripInfoMap.put("scheduler", scheduler.name());
					}

					JOB_TYPE jobtype = gatewayentry.getJobType();
					if (jobtype != null) {
						gripInfoMap.put("jobtype", jobtype.name());
					}

					os = gatewayentry.getOS();
					if (os != null) {
						gripInfoMap.put("os", os.name());
					}

					osrelease = gatewayentry.getOSRelease();
					if (osrelease != null) {
						gripInfoMap.put("osrelease", osrelease);
					}

					osversion = gatewayentry.getOSVersion();
					if (osversion != null) {
						gripInfoMap.put("osversion", osversion);
					}

					glibc = gatewayentry.getGlibc();
					if (glibc != null) {
						gripInfoMap.put("glibc", glibc);
					}

					int idle_nodes = gatewayentry.getIdleNodes();
					if (idle_nodes != -1) {
						gripInfoMap.put("idle-nodes", idle_nodes);
					}

					int total_nodes = gatewayentry.getTotalNodes();
					if (total_nodes != -1) {
						gripInfoMap.put("total-nodes", total_nodes);
					}

					gateways.add(gripInfoMap);
				}

				List<Object> replicaCatalogs = new LinkedList<Object>();

				Iterator<ReplicaCatalog> catalogIter = entry.getReplicaCatalogIterator();

				if (catalogIter.hasNext()) {
					siteCatalogInfo.put("replica-catalog", replicaCatalogs);
				}

				while (catalogIter.hasNext()) {

					ReplicaCatalog cataloginfo = catalogIter.next();

					Map<String, Object> replicationCatalogInfo = new LinkedHashMap<String, Object>();

					String type = cataloginfo.getType();
					if (type != null) {
						replicationCatalogInfo.put("type", type);
					}

					String url = cataloginfo.getURL();
					if (url != null) {
						replicationCatalogInfo.put("url", url);
					}

					List<String> alias = new LinkedList<>();

					Iterator<String> aliasIter = cataloginfo.getAliasIterator();
					if (aliasIter.hasNext()) {
						replicationCatalogInfo.put("alias", alias);
					}

					while (aliasIter.hasNext()) {
						alias.add(aliasIter.next());
					}

					List<Object> connections = new LinkedList<>();

					Iterator<Connection> connectionsIter = cataloginfo.getConnectionIterator();
					if (connectionsIter.hasNext()) {
						replicationCatalogInfo.put("connection", connections);
					}

					while (connectionsIter.hasNext()) {
						Connection con = connectionsIter.next();
						Map<String, String> connectionMap = new LinkedHashMap<>();
						connectionMap.put("key", con.getKey());
						connectionMap.put("value", con.getValue());
						connections.add(connectionMap);
					}

					replicaCatalogs.add(replicationCatalogInfo);
				}

				if (entry.getProfiles() != null) {
					List<Map<String, Map<String, Object>>> profileData = buildProfiles(
							entry.getProfiles().getProfiles());
					if (profileData != null && profileData.size() > 0) {
						siteCatalogInfo.put("profile", profileData);
					}
					Map<String, Object> metaData = buildMeta(entry.getProfiles().getProfiles());
					if (metaData != null && metaData.size() > 0) {
						siteCatalogInfo.put("metadata", metaData);
					}
				}
				siteCatalogs.add(siteCatalogInfo);
			}
			mapper.writeValue(out, siteData);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	/**
	 * This helper method is used to build the profiles from the existing profile..
	 * Profiles will have meta also, omit this..
	 * 
	 * @param profiles - List of profiles..
	 * @return List<Map<String, Map<String, Object>>> because of the following
	 *         format: profile: - env: APP_HOME: "/tmp/mukund" JAVA_HOME:
	 *         "/bin/java.1.6" me: "with" - condor: more: "test"
	 * 
	 */
	private static List<Map<String, Map<String, Object>>> buildProfiles(List<Profile> profiles) {
		List<Map<String, Map<String, Object>>> profileList = new LinkedList<Map<String, Map<String, Object>>>();
		for (Profile profile : profiles) {
			String nameSpace = profile.getProfileNamespace();
			if (!nameSpace.contains("meta")) {
				String key = profile.getProfileKey();
				String value = profile.getProfileValue();
				getMapForProfile(nameSpace, profileList).put(key, value);
			}
		}
		return profileList;
	}

	/**
	 * This method extracts and builds the meta data information.
	 * 
	 * @param profiles - List of profiles..
	 * @return Map<String, Object> - Simple key value inforamtion of meta.
	 */
	private static Map<String, Object> buildMeta(List<Profile> profiles) {
		Map<String, Object> metaMap = new HashMap<String, Object>();
		for (Profile profile : profiles) {
			String nameSpace = profile.getProfileNamespace();
			if (nameSpace.contains("meta")) {
				String key = profile.getProfileKey();
				String value = profile.getProfileValue();
				metaMap.put(key, value);
			}
		}
		return metaMap;
	}

	private static Map<String, Object> getMapForProfile(String nameSpace,
			List<Map<String, Map<String, Object>>> profileList) {
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
					Map<String, Map<String, Object>> mapsTemp = new HashMap<>();
					mapsTemp.put(nameSpace, keyValueMap);
					profileList.add(mapsTemp);
					return keyValueMap;
				}
			}
		}
		return null;

	}

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		LogManager logger = LogManagerFactory.loadSingletonInstance();
		PegasusProperties properties = PegasusProperties.nonSingletonInstance();
		logger.setLevel(5);
		logger.logEventStart("test.parser", "dax", null);

		List s = new LinkedList();
		s.add("*");
		PegasusBag bag = new PegasusBag();
		bag.add(PegasusBag.PEGASUS_LOGMANAGER, logger);
		bag.add(PegasusBag.PEGASUS_PROPERTIES, properties);

		List<String> files = new LinkedList<String>();
		files.add("/home/mukund/pegasus/test/junit/edu/isi/pegasus/planner/catalog/site/impl/input/sites.yaml");

		for (String file : files) {
			SiteCatalogYAMLParser parser = new SiteCatalogYAMLParser(bag, s);
			System.out.println(" *********Parsing File *********" + file);
			parser.startParser(file);
			SiteStore store = parser.getSiteStore();
			// System.out.println( store );

			SiteCatalogEntry entry = store.lookup("local");

			System.out.println(entry);
			SiteDataVisitor visitor = new XML4PrintVisitor();
			StringWriter writer = new StringWriter();
			visitor.initialize(writer);

			try {
				store.accept(visitor);
				System.out.println("Site Catalog is \n" + writer.toString());
			} catch (IOException ex) {
				Logger.getLogger(SiteCatalogYAMLParser.class.getName()).log(Level.SEVERE, null, ex);
			}

			System.out.println(" *********Parsing Done *********");
		}

//        System.out.println( Directory.TYPE.value( "shared-scratch" ));
	}
}
