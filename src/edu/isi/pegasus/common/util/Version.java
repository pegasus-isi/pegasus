/*
 * This file or a portion of this file is licensed under the terms of
 * the Globus Toolkit Public License, found in file GTPL, or at
 * http://www.globus.org/toolkit/download/license.html. This notice must
 * appear in redistributions of this file, with or without modification.
 *
 * Redistributions of this Software, with or without modification, must
 * reproduce the GTPL in: (1) the Software, or (2) the Documentation or
 * some other similar material which is provided with the Software (if
 * any).
 *
 * Copyright 1999-2004 University of Chicago and The University of
 * Southern California. All rights reserved.
 */
package edu.isi.pegasus.common.util;

import edu.isi.pegasus.planner.catalog.classes.SysInfo;
import java.io.*;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * This class solely defines the version numbers of PEGASUS. The template
 * file will be substituted by ant during the built process to compile
 * in the built timestamp.<p>
 *
 * When using the class, the methods for the version number digits
 * will always return an integer. In order to obtain the full version
 * number, including any "release candidate" suffices, please prefer
 * the <code>toString</code> method over combining the separate version
 * integers yourself.
 *
 * @author Karan Vahi
 * @author Jens-S. Vöckler
 * @author Gideon Juve
 */
public class Version {
    /**
     * Basename of the build stamp file.
     */
    public static final String STAMP_FILENAME = "stamp";
    
    private static final String mPlatformRegex =
                                        "(x86|x86_64|ia64|ppc)_([a-zA-Z0-9]*)_([0-9]*)";

    /**
     * Stores compiled patterns at first use, quasi-Singleton.
     */
    private static Pattern mPlatformPattern = null;
    
    private Properties props = new Properties();
    
    public Version() {
        try {
            props.load(Version.class.getClassLoader().getResourceAsStream("pegasus.build.properties"));
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to load pegasus build properties", ioe);
        }
        //compile the pattern only once.
        if( mPlatformPattern == null ){
             mPlatformPattern = Pattern.compile( mPlatformRegex );
        }
    }

    /**
     * @deprecated Just create a new instance
     */
    public static Version instance() {
        return new Version();
    }

    public String toString() {
        return getVersion();
    }

    /**
     * @return The pegasus version
     */
    public String getVersion() {
        return props.getProperty("pegasus.build.version");
    }

    private String[] splitVersion() {
        return getVersion().split("[.]");
    }

    /**
     * @return Major version number
     */
    public String getMajor() {
        return splitVersion()[0];
    }

    /**
     * @return Minor version number
     */
    public String getMinor() {
        return splitVersion()[1];
    }

    /**
     * @return Patch version number plus other tags
     */
    public String getPatch() {
        return splitVersion()[2];
    }

    /**
     * @return the formatted time stamp of the build.
     * @deprecated Use getTimestamp() instead
     */
    public String determineBuilt() {
        return getTimestamp();
    }

    /**
     * @return the build timestamp
     */
    public String getTimestamp() {
        return props.getProperty("pegasus.build.timestamp");
    }

    /**
     * Determines the build platform.
     * @return an identifier for the build platform.
     * @deprecated Used getPlatform() instead
     */
    public String determinePlatform() {
        return getPlatform();
    }

    /**
     * @return The build platform
     */
    public String getPlatform() {
        return props.getProperty("pegasus.build.platform");
    }
    
    /**
     * Return the architecture 
     * 
     * @return 
     */
    public SysInfo.Architecture getArchitecture(){
       String platform = this.getPlatform();
       Matcher matcher = mPlatformPattern.matcher( platform );
       String arch = null;
       if(  matcher.matches() ){
             arch = matcher.group(1);
       }
       else{
          throw new RuntimeException( "Unable to determine architecture from  " + platform );
       }
       return SysInfo.Architecture.valueOf(arch);
 
    }
    
    /**
     * Return the OS 
     * 
     * @return 
     */
    public SysInfo.OS getOS(){
        return SysInfo.computeOS( this.getOSRelease() );
    }
    
    
    
    /**
     * Return the OS Release
     * 
     * @return 
     */
    public SysInfo.OS_RELEASE getOSRelease(){
       String platform = this.getPlatform();
       Matcher matcher = mPlatformPattern.matcher( platform );
       String release = null;
       if(  matcher.matches() ){
             release = matcher.group(2);
       }
       else{
          throw new RuntimeException( "Unable to determine OS Release from  " + platform );
       }
       return SysInfo.OS_RELEASE.valueOf(release);
 
    }
    

    /**
     * @return The Git hash that this version of Pegasus was built from
     */
    public String getGitHash() {
        return props.getProperty("pegasus.build.git.hash");
    }

    /**
     * Determines the built and architecture of the installation. These
     * are usually separated by a linear white-space.
     *
     * @return the formatted time stamp of the built, if available, and an
     * identifier for the architecture. An string indicating that the
     * build is unknown is returned on failure.
     */
    public String determineInstalled() {
        String result = "unknown unknown";
        String pegasushome = System.getProperty( "pegasus.home" );
        if ( pegasushome != null ) {
            try {
                File stampfile = new File( pegasushome, STAMP_FILENAME );
                if ( stampfile.canRead() ) {
                    BufferedReader br = new BufferedReader( new FileReader(stampfile) );
                    String built = br.readLine();
                    br.close();
                    if ( built != null && built.length() > 0 ) result = built;
                }
            } catch ( IOException ioe ) {
                // ignore
            }
        }

        return result;
    }

    /**
     * Determines, if the compiled version and the installed version
     * match. The match is done by comparing the timestamps and
     * architectures.
     *
     * @return true, if versions match, false otherwise.
     */
    public boolean matches() {
        String s[] = determineInstalled().split("\\s+");
        return ( s.length >= 2 &&
                 s[0].equalsIgnoreCase( determineBuilt() ) &&
                 s[1].equalsIgnoreCase( determinePlatform() ) );
    }
}

