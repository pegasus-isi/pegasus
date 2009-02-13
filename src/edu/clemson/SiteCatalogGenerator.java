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

package edu.clemson;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import java.text.SimpleDateFormat;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 
 * @author Vikas Patel vikas@vikaspatel.org
 */
public class SiteCatalogGenerator {

    private ArrayList<String> outputArray;
    private FileWriter fileWriter;
    private int siteCount = 0;

    
    /**
     * The sites that need to be parsed
     */
    private Set<String> mSites;
    
    /**
     * 
     * @param outputArray
     * @param fileWriter
     */
    public SiteCatalogGenerator(ArrayList<String> outputArray, FileWriter fileWriter) {
        this.outputArray = outputArray;
        this.fileWriter = fileWriter;
    }
    
    /**
     * Overloaded Constructor for passing the list of sites to load.
     * 
     * The site handle * is a special handle designating all sites are to be 
     * loaded.
     * 
     * @param  outputArray
     * @param  sites       the sites to be parsed.
     * 
     * @return
     */
    public SiteCatalogGenerator( ArrayList<String> outputArray){
        this.outputArray = outputArray;
        
    }
    
    
    /**
     * 
     * Loads the information about the sites that are passed from the condor-status
     * output.
     * 
     * The site handle * is a special handle designating all sites are to be 
     * loaded.
     * 
     * @param  sites       the sites to be parsed.
     * 
     * 
     * @return List of SiteCatalogEntry objects containing the site information for loaded
     *         sites.
     */
    public List<Site> loadSites( List<String> sites )  {
        List<Site> result = new ArrayList<Site>();
        this.mSites  = new HashSet<String>( sites );
        boolean all = mSites.contains( "*" );
        for (int i = 0; !outputArray.isEmpty() && i < outputArray.size(); i++) {
            String line = outputArray.get(i);
            if (!(line == null || line.equals(""))) {
                Site site = parseSiteInfo(line);
                if( all || mSites.contains( site.siteName ) ){
                    result.add( site );
                }
            }
        }
        return result;
    }
    

    private Site parseSiteInfo(String line) {
        Site site = new Site();
        try {
            String[] siteInfoArray = new String[10];

            int siteInfoArrayIndex = 0;

            site.siteName = siteInfoArray[0];

            String line2 = line;
            int startPos = 0, endPos = 0;
            while (!line2.equals("")) {
                endPos = line2.indexOf(";");
                siteInfoArray[siteInfoArrayIndex++] = line2.substring(startPos, endPos);

                line2 = line2.substring(endPos + 1); //endPos refers to ";"
                startPos = 0;

            }

            if (siteInfoArray[0] != null && !siteInfoArray[0].equals("")) {
                site.siteName = siteInfoArray[0];
            } else {
                return site;
            }
            String globusLocation = null;
            if (siteInfoArray[1] != null && !siteInfoArray[1].equals("") && siteInfoArray[1].lastIndexOf("/") != -1 && !siteInfoArray[1].equals("UNAVAILABLE")) {
                globusLocation = siteInfoArray[1];
            } else {
                //System.out.println(site.siteName+": Error fetching globusLocation from ... Ignoring site");
                return site;
            }



            if (globusLocation != null) {
                site.globusLocation = globusLocation;
                site.globusLib = site.globusLocation + "/lib";
                int i = globusLocation.lastIndexOf("/");
                String temp = globusLocation.substring(i);

                if (temp.equals("")) /* Incase '/' is the last character of the string */ {
                    i = i - 1;
                }

                //Commented by Karan
                //System.out.println(site.siteName);
                String osgLocation = globusLocation.substring(0, i);
                site.pegasusHome = osgLocation + "/pegasus";
                site.gridlaunch = site.pegasusHome + "/bin/kickstart";
            }

            if (siteInfoArray[2] != null && !siteInfoArray[2].equals("")) {
                String jobmanager = siteInfoArray[2];
                site.VanillaUniverseJobManager = jobmanager;
                site.transferUniverseJobManager = jobmanager.substring(0, jobmanager.indexOf("/jobmanager")) + "/jobmanager-fork";
            }

            if (siteInfoArray[3] != null && !siteInfoArray[3].equals("")) {
                site.workingDirectory = siteInfoArray[3];

            } else {
                site.workingDirectory = "/tmp";
            }

            site.gridFtpStorage = site.workingDirectory;
            if (siteInfoArray[4] != null && !siteInfoArray[4].equals("")) {
                site.gridFtpUrl = "gsiftp"/*siteInfoArray[4]*/ + "://" + siteInfoArray[4];
            }

            if ((siteInfoArray[5] != null && !siteInfoArray[5].equals("")) &&
                    !siteInfoArray[5].equals("UNAVAILABLE")) {
                site.app = siteInfoArray[5];
            }
            if ((siteInfoArray[6] != null && !siteInfoArray[6].equals("")) &&
                    !siteInfoArray[6].equals("UNAVAILABLE")) {
                site.data = siteInfoArray[6];
            }
            if ((siteInfoArray[7] != null && !siteInfoArray[7].equals("")) &&
                    !siteInfoArray[7].equals("UNAVAILABLE")) {
                site.tmp = siteInfoArray[7];
            }
            if ((siteInfoArray[8] != null && !siteInfoArray[8].equals("")) &&
                    !siteInfoArray[8].equals("UNAVAILABLE")) {
                site.wntmp = siteInfoArray[8];
            }


        } catch (ArrayIndexOutOfBoundsException e) {
            e.printStackTrace();
        }
        return site;
    }

    public void generateSiteCatalog() throws IOException {
        //fileWriter= new FileWriter(file);
        addHeaderInformationToSiteCatalog();
        int outputArraySize = outputArray.size();
        for (int i = 0; !outputArray.isEmpty() && i < outputArraySize; i++) {
            String line = outputArray.get(i);
            if (!(line == null || line.equals(""))) {
                Site site = parseSiteInfo(line);
                addSiteToCatalog(site);
            }
        }
        addSiteToCatalog(addLocalSiteInformation());
        endSiteCatalog(fileWriter);
    }

    private void addSiteToCatalog(Site site) throws IOException {
        if (site.globusLocation == null || site.VanillaUniverseJobManager == null) {
            return;
        }
        String comment = "<!-- Site: " + (++siteCount) + " -->";
        String siteBody;
        siteBody = "\n<site handle=" + "\"" + site.siteName + "\"" +
                " gridlaunch=" + "\"" + site.gridlaunch + "\"" + " sysinfo=" + "\"" + site.sysinfo + "\"" + ">\n" +
                " <profile namespace=" + "\"" + "env" + "\"" + " key=" + "\"" + "PEGASUS_HOME" + "\"" + ">" + site.pegasusHome + "</profile>\n" +
                " <profile namespace=" + "\"" + "env" + "\"" + " key=" + "\"" + "GLOBUS_LOCATION" + "\"" + ">" + site.globusLocation + "</profile>\n" +
                " <profile namespace=" + "\"" + "env" + "\"" + " key=" + "\"" + "LD_LIBRARY_PATH" + "\"" + ">" + site.globusLib + "</profile>\n";

        if (site.app != null) {
            siteBody += " <profile namespace=\"env\" key=\"app\">" + site.app + "</profile>\n";
        }
        if (site.data != null) {
            siteBody += " <profile namespace=\"env\" key=\"data\">" + site.data + "</profile>\n";
        }
        if (site.tmp != null) {
            siteBody += " <profile namespace=\"env\" key=\"tmp\">" + site.tmp + "</profile>\n";
        }
        if (site.wntmp != null) {
            siteBody += " <profile namespace=\"env\" key=\"wntmp\">" + site.wntmp + "</profile>\n";
        }
        siteBody += " <lrc url=" + "\"" + site.lrcUrl + "\"" + "/>\n" +
                " <gridftp  url=" + "\"" + site.gridFtpUrl + "\"" + " storage=" + "\"" + site.gridFtpStorage + "\" major=\"4\" minor=\"0\" patch=\"3\"/>\n" +
                " <jobmanager universe=\"transfer\" url=\"" + site.transferUniverseJobManager + "\" major=\"4\" minor=\"0\" patch=\"3\" />\n" +
                " <jobmanager universe=\"vanilla\" url=\"" + site.VanillaUniverseJobManager + "\" major=\"4\" minor=\"0\" patch=\"3\" />\n" +
                " <workdirectory >" + site.workingDirectory + "</workdirectory>\n" +
                "</site>\n\n";

        fileWriter.write(comment + siteBody);
        fileWriter.flush();

    }

    private void addHeaderInformationToSiteCatalog() throws IOException {
        String header = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "<!--............................................................ \n''Pegasus Site catalog''\n" +
                "Created on " + (new SimpleDateFormat("dd MMMMM yyyy 'at' hh:mm:ss z")).format(Calendar.getInstance().getTime()) +
                "\nCreated by SiteWriter [Clemson CyberInfrastructure Research Group(CIRG)]\n" +
                "Created from ReSS/RENCI glueClassAds\n" +
                "...................................................................-->" +
                "\n<sitecatalog xmlns=\"http://pegasus.isi.edu/schema/sitecatalog\" " +
                "xsi:schemaLocation=\"http://pegasus.isi.edu/schema/sitecatalog " +
                "http://pegasus.isi.edu/schema/sc-2.0.xsd\" " +
                "xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\" version=\"2.0\">\n";

        fileWriter.write(header);
        fileWriter.flush();
    }

    private void endSiteCatalog(FileWriter fileWriter) throws IOException {
        fileWriter.write("</sitecatalog>");
        fileWriter.flush();
    }

    Site addLocalSiteInformation() {
        Site site = new Site();
        site.siteName = "local";
        String globusLocation = null;
        String home = null;
        try {
            globusLocation = System.getenv("GLOBUS_LOCATION"); //System.getenv :Undeprecated after jdk 1.4
            home = System.getenv("HOME");
        } catch (Exception e) {
            System.out.println("\nWARNING: \nError occured getting 'local' site information, please make sure GLOBUS_LOCATION and HOME " +
                    "environment variables are set");
            System.out.println("Skipping local site");
        }

        site.globusLocation = globusLocation;
        site.globusLib = globusLocation + "/lib";


        if (globusLocation == null) {
            System.out.println("\nWARNING: \nError occured getting 'local' site information, please make sure GLOBUS_LOCATION and HOME " +
                    "environment variables are set");
            System.out.println("Skipping local site");
        } else {
            int i = globusLocation.lastIndexOf("/");
            String temp = globusLocation.substring(i);

            if (globusLocation.equals("")) /* Incase '/' is the last character of the string */ {
                i = i - 1;
                temp = globusLocation.substring(i);
            }

            String osgLocation = globusLocation.substring(0, i);

            site.pegasusHome = osgLocation + "/pegasus";
            site.gridlaunch = site.pegasusHome + "/bin/kickstart";
        }

        if (home != null) {
            File file = new File(home + File.separator + "vdldemo");
            if (!file.exists()) {
                file.mkdir();
            }
            site.workingDirectory = home + File.separator + "vdldemo";
            site.gridFtpStorage = site.workingDirectory;
        }

        String hostname = null;
        try {
            hostname = java.net.InetAddress.getLocalHost().getHostName();

        } catch (java.net.UnknownHostException e) {
            System.out.println("\nWARNING:\nError retrieving local site information...");
            System.out.println("Skipping local site...");
        }

        if (hostname != null) {
            String jobmanager = hostname + "/jobmanager-condor";
            site.VanillaUniverseJobManager = jobmanager;
            site.transferUniverseJobManager = jobmanager.substring(0, jobmanager.indexOf("/jobmanager")) + "/jobmanager-fork";
            site.gridFtpUrl = "gsiftp://" + jobmanager.substring(0, jobmanager.indexOf("/jobmanager"));
        }

        return site;

    }

    public class Site {

        public String siteName;
        public String sysinfo = "INTEL32::LINUX"; //default value
        public String globusLocation;
        public String globusLib;
        public String gridlaunch;
        public String pegasusHome;
        public String lrcUrl = "rlsn://dummyValue.url.edu";
        public String gridFtpUrl;
        public String gridFtpStorage;
        public String transferUniverseJobManager;
        public String VanillaUniverseJobManager;
        public String workingDirectory;
        public String app;
        public String data;
        public String tmp;
        public String wntmp;
    }



    
}





