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

import edu.clemson.SiteCatalogGenerator;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;



/**
 * 
 * @author Vikas Patel vikas@vikaspatel.org
 */
public class SiteWriterMain {

			
	private String command="condor_status -any -pool engage-central.renci.org " +
			               "-format %s GlueSiteName " +
			               "-format ; 1 "+                         // to force a semicolon, even if the attribute was not found
			               "-format %s OSGMM_Globus_Location "+
			               "-format ; 1 "+
			               "-format %s GlueCEInfoContactString "+
			               "-format ; 1 "+
			               "-format %s GlueClusterTmpDir "+
			               "-format ; 1 "+			      
			               "-format %s GlueCEInfoHostName "+
			               "-format ; 1 "+
			               "-format %s GlueCEInfoApplicationDir "+
			               "-format ; 1 "+
			               "-format %s GlueCEInfoDataDir "+
			               "-format ; 1 "+
			               "-format %s GlueClusterTmpDir "+
			               "-format ; 1 "+
			               "-format %s GlueClusterWNTmpDir "+ 
					       "-format ;\\n 1 ";
	
	
	private File file=null;
	ArrayList<String> outputArray=new ArrayList<String>();
	ArrayList<String> errorArray=new ArrayList<String>();
	
	public static void main(String[] args) 
	{
	   
		SiteWriterMain siteWriter=new SiteWriterMain();
		
		
		if(args.length>0)
		{			
			if(args[0].startsWith("--"))
				siteWriter.argumentHandler(args[0]);
			else
				siteWriter.getSitesInfo(args[0]);
		}
		else 
			siteWriter.getSitesInfo("sites.xml");
	}
	
	
	private void getSitesInfo(String sitesFileName)
	{
		printTitle();
				
		try
	       {
		     	
		    	Runtime runtime=Runtime.getRuntime();  
		        Process process=runtime.exec(command);
	
		        StreamGobbler outputGobbler=new StreamGobbler(process.getInputStream(),this.outputArray);
		        StreamGobbler errorGobbler=new StreamGobbler(process.getErrorStream(), this.errorArray);
		        outputGobbler.start();
		        errorGobbler.start(); 
		         
		        outputGobbler.join();
		        errorGobbler.join();
		             
		                  
			    int result=process.waitFor(); 
		}
	       catch(IOException e)
	       {
	    	   System.out.println("\n\nERROR OCCURED:");
	    	   System.out.println(" condor_status not found \n This program requires Condor (http://www.cs.wisc.edu/condor/)\n");
	    	   return;
	       }
	       catch(Exception e)
	       {
	    	 System.out.println("\n\nERROR OCCURED:");
	    	 System.out.println("Exiting program: Please run the program again.");
	    	 return;
	       }
	       
	     
	       
	    if(sitesFileName==null || sitesFileName.equals(""))
			sitesFileName="sites.xml";
			
		FileWriter fileWriter;
		try
			{
				 file=new File(sitesFileName);
				 if(file.exists())
				 {
					 int counter=0;
					 String tmpFileName=sitesFileName+".bkp."+counter;
					 
					 File tmpFile;
					 
					 while((tmpFile=new File(tmpFileName+counter)).exists())
					 {
						 counter++;
					 }
					
					System.out.println("Warning: The file "+sitesFileName+" already exists");
					System.out.println("Renaming the existing file to "+tmpFileName+counter);
					file.renameTo(tmpFile);
					 
				 }
				 
				 fileWriter= new FileWriter(file);
			}
			catch(IllegalArgumentException e)
			{
				System.out.println("Invalid argument, enter a valid path/name");
				return;
			}
			catch(IOException e)
			{
				System.out.println("Invalid argument, enter a valid path/name");
				System.out.println("Exiting program, please try again ....");
				return;
			}
		
		    System.out.println("Discovering sites from RENCI, writing to site catalog\n");
		    try 
		    {
				new SiteCatalogGenerator(outputArray,fileWriter).generateSiteCatalog();
			} 
		    catch (Exception e) 
		    {
		    	System.out.println("\nERROR OCCURED: Please try again !!\n");
		    	System.out.println("Please ensure that Condor (http://www.cs.wisc.edu/condor/) is correctly installed and condor_status is not firewalled.");
		    	System.out.println("If the problem persists please contact vikasp@clemson.edu.");
		    	System.out.println("Note: the site catalog (ex. sites.xml) maybe incomplete, if you already had a site catalog file it was backed up for you.");
		    	return;
			}
		    
		    System.out.println("\nFinished...");  
	       
}
	private void argumentHandler(String arg)
	{
		printTitle();
		if(arg.equals("--help"))
		{	
			System.out.println("SYNTAX:");
			System.out.println("\n SiteWriter <site catalog name or path>");
		}
		else
		{	System.out.println("Invalid argument");
			System.out.println("SYNTAX:");
		    System.out.println("\n SiteWriter <site catalog name or path>");
		}
	}
	private void printTitle()
	{
		System.out.println("...................................................");
	    System.out.println("\tSiteWriter, CIRG");
	    System.out.println("....................................................\n");
	}
}