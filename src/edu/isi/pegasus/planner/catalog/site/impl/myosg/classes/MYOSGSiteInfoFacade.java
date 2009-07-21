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
package edu.isi.pegasus.planner.catalog.site.impl.myosg.classes;

import java.util.Iterator;
import java.util.List;

import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.MYOSGSiteConstants;

public class MYOSGSiteInfoFacade {
	MYOSGSiteInfo myOsgSiteInfo;
	
	private String shortname; 				//shortname
	private String app_loc;      			//app_loc
	private String data_loc;     			//data_loc
	private String osg_grid;    			//osg_grid
	private String wntmp_loc;              	//wntmp_loc
	private String tmp_loc;      			//tmp_loc
	private String gatekeeper;			 	//gatekeeper
	private String gk_port;     			//gk_port	
	private String gsiftp_server;           // gk_server
	private String gsiftp_port;         	//gsiftp_port	
	private String sponsor_vo;             	//sponsor_vo
	private String vdt_version; 			//vdt_version
	private String globus_loc;              //globus_loc  --if globus_loc is empty then use osg_grid
	private String exec_jm; 				//exec_jm -- compute
	private String util_jm;                 //util_jm -- transfer
	private String grid_services;           //grid_services
	private String app_space;				//app_space -- saved for later use
	private String data_space;				//data_space -- saved for later use
	private String tmp_space;				//tmp_space -- saved for later use
	
	public MYOSGSiteInfoFacade(MYOSGSiteInfo myOsgSiteInfo){
		this.myOsgSiteInfo = myOsgSiteInfo;
		init(this.myOsgSiteInfo);
		
	}
	
	public boolean isValidSite(){
		if(app_loc.equals("") || data_loc.equals("") || globus_loc.equals("")|| exec_jm.equals("")){
			return false;
		}
		return true;
	}
	
	private void init(MYOSGSiteInfo myOsgSiteInfo){
		 List <MYOSGSiteResourceInfo> myOSGSiteResourceInfoList = (List <MYOSGSiteResourceInfo>)myOsgSiteInfo.getProperty(MYOSGSiteConstants.RESOURCE_LIST_ID);
		 MYOSGSiteResourceInfo myOSGSiteResourceInfo = myOSGSiteResourceInfoList.get(0);
		 List <MYOSGSiteServiceInfo> myOSGSiteServiceInfoList = (List <MYOSGSiteServiceInfo>)myOSGSiteResourceInfo.getProperty(MYOSGSiteConstants.SERVICE_LIST_ID);
		 MYOSGSiteServiceInfo myOSGSiteServiceInfoCE = getCE(myOSGSiteServiceInfoList);
		 MYOSGSiteServiceInfo myOSGSiteServiceInfoGridFtp = getGridFtp(myOSGSiteServiceInfoList);
		 MYOSGSiteEnvironmentInfo myOSGSiteEnvironmentInfo = (MYOSGSiteEnvironmentInfo)myOSGSiteResourceInfo.getProperty(MYOSGSiteConstants.ENVIRONMENT_INFO_ID);
		 MYOSGSiteVOOwnershipInfo myOSiteVOOwnershipInfo = (MYOSGSiteVOOwnershipInfo)myOSGSiteResourceInfo.getProperty(MYOSGSiteConstants.VO_OWNERSHIP_INFO_ID);
		 shortname = (String)myOsgSiteInfo.getProperty(MYOSGSiteConstants.SITE_NAME_ID); 				//shortname
		 app_loc  = (String)myOSGSiteEnvironmentInfo.getProperty(MYOSGSiteConstants.ENV_APP_ID);      		//app_loc
		 data_loc = (String)myOSGSiteEnvironmentInfo.getProperty(MYOSGSiteConstants.ENV_DATA_ID);     		//data_loc
		 osg_grid = (String)myOSGSiteEnvironmentInfo.getProperty(MYOSGSiteConstants.ENV_GRID_ID);      		//osg_grid
		 wntmp_loc = (String)myOSGSiteEnvironmentInfo.getProperty(MYOSGSiteConstants.ENV_WN_TMP_ID);               //wntmp_loc
		 tmp_loc = "/tmp";       			//tmp_loc
		 gatekeeper = (String)myOSGSiteEnvironmentInfo.getProperty(MYOSGSiteConstants.ENV_JOB_CONTACT_ID);  			 	//gatekeeper
		 if(myOSGSiteServiceInfoCE == null || ((String)myOSGSiteServiceInfoCE.getProperty(MYOSGSiteConstants.SERVICE_URI_ID)).equals("")){
			 gk_port = ((String)myOSGSiteServiceInfoCE.getProperty(MYOSGSiteConstants.SERVICE_URI_ID)).split(":")[1];    //(DEFAULT to 2119)   			//gk_port
		 }
		 else{
			 gk_port ="2119";
		 }
		 if(myOSGSiteServiceInfoGridFtp == null || ((String)myOSGSiteServiceInfoGridFtp.getProperty(MYOSGSiteConstants.SERVICE_URI_ID)).equals("")){
			 gsiftp_server =""; 
		 }else{
			 gsiftp_server = ((String)myOSGSiteServiceInfoGridFtp.getProperty(MYOSGSiteConstants.SERVICE_URI_ID));
		 }
		 
		 if(myOSGSiteServiceInfoGridFtp == null || ((String)myOSGSiteServiceInfoGridFtp.getProperty(MYOSGSiteConstants.SERVICE_URI_ID)).equals("")){
			 gsiftp_port = "2811";
		 }
		 else{
			 gsiftp_port = ((String)myOSGSiteServiceInfoGridFtp.getProperty(MYOSGSiteConstants.SERVICE_URI_ID)).split(":")[1]; // DEFAULT to 2811           	//gsiftp_port
		 }
		 
		 sponsor_vo = (myOSiteVOOwnershipInfo == null)?"":(String)myOSiteVOOwnershipInfo.getProperty(MYOSGSiteConstants.VO_OWN_VO_ID);               //sponsor_vo // GET from VO ownership
		 vdt_version = "";  			//vdt_version
		 globus_loc = (String)myOSGSiteEnvironmentInfo.getProperty(MYOSGSiteConstants.ENV_GLOBUS_LOCATION_ID);              //globus_loc  --if globus_loc is empty then use osg_grid
		 exec_jm = (String)myOSGSiteEnvironmentInfo.getProperty(MYOSGSiteConstants.ENV_JOB_CONTACT_ID);  				//exec_jm -- compute
		 util_jm = getUtilJobManager(exec_jm);                //util_jm -- transfer
		 grid_services = "";             //grid_services
		 app_space = "";  				//app_space -- saved for later use
		 data_space = "";  				//data_space -- saved for later use
		 tmp_space = "";  				//tmp_space -- saved for later use
	}
	
	/**
	 * Returns the utility job manager path
	 * @param exec_jm execute job manager path
	 * @return utility job manager path
	 */
	private String getUtilJobManager(String exec_jm){
		String jobManager ="";
		if(exec_jm.equals(""))
			return jobManager;
		int index = exec_jm.lastIndexOf("/");
		if(index != -1){
			jobManager = exec_jm.substring(0, index) +"/jobmanager-fork";
		}
		return jobManager;
	}
	
	/**
	 * Returns the compute element(CE)
	 * @param myOSGSiteServiceInfoList service list
	 * @return compute element
	 */
	private MYOSGSiteServiceInfo getCE(
			List<MYOSGSiteServiceInfo> myOSGSiteServiceInfoList) {

		Iterator<MYOSGSiteServiceInfo> iterator = myOSGSiteServiceInfoList
				.iterator();
		while (iterator.hasNext()) {
			MYOSGSiteServiceInfo serviceInfo = iterator.next();
			if (serviceInfo.getProperty(MYOSGSiteConstants.SERVICE_NAME_ID)
					.equals("CE"))
				return serviceInfo;
		}
		return null;

	}
	/**
	 * Returns the GridFtp 
	 * @param myOSGSiteServiceInfoList service list
	 * @return Grid Ftp
	 */
	private MYOSGSiteServiceInfo getGridFtp(
			List<MYOSGSiteServiceInfo> myOSGSiteServiceInfoList) {

		Iterator<MYOSGSiteServiceInfo> iterator = myOSGSiteServiceInfoList
				.iterator();
		while (iterator.hasNext()) {
			MYOSGSiteServiceInfo serviceInfo = iterator.next();
			if (serviceInfo.getProperty(MYOSGSiteConstants.SERVICE_NAME_ID)
					.equals("GridFtp"))
				return serviceInfo;
		}
		return null;

	}
	
	public void print(){
		System.out.println("***********************************************");
		System.out.println("shortname " + shortname);		
		System.out.println("app_loc " + app_loc);
		System.out.println("data_loc " + data_loc);
		System.out.println("osg_grid " + osg_grid);
		System.out.println("tmp_loc " + tmp_loc);
		System.out.println("wntmp_loc " + wntmp_loc);
		System.out.println("gatekeeper " + gatekeeper);
		System.out.println("gk_port " + gk_port);
		System.out.println("gsiftp_port " + gsiftp_port);
		System.out.println("sponsor_vo " + sponsor_vo);
		System.out.println("vdt_version " + vdt_version);
		System.out.println("globus_loc " + globus_loc);
		System.out.println("exec_jm " + exec_jm);
		System.out.println("util_jm " + util_jm);
		System.out.println("grid_services " + grid_services);
		System.out.println("app_space " + app_space);
		System.out.println("data_space " + data_space);
		System.out.println("tmp_space " + tmp_space);
		System.out.println("***********************************************");
	}
	public String getShortname() {
		return shortname;
	}
	public void setShortname(String shortname) {
		this.shortname = shortname;
	}
	public String getApp_loc() {
		return app_loc;
	}
	public void setApp_loc(String app_loc) {
		this.app_loc = app_loc;
	}
	public String getData_loc() {
		return data_loc;
	}
	public void setData_loc(String data_loc) {
		this.data_loc = data_loc;
	}
	public String getOsg_grid() {
		return osg_grid;
	}
	public void setOsg_grid(String osg_grid) {
		this.osg_grid = osg_grid;
	}
	public String getWntmp_loc() {
		return wntmp_loc;
	}
	public void setWntmp_loc(String wntmp_loc) {
		this.wntmp_loc = wntmp_loc;
	}
	public String getTmp_loc() {
		return tmp_loc;
	}
	public void setTmp_loc(String tmp_loc) {
		this.tmp_loc = tmp_loc;
	}
	public String getGatekeeper() {
		return gatekeeper;
	}
	public void setGatekeeper(String gatekeeper) {
		this.gatekeeper = gatekeeper;
	}
	public String getGk_port() {
		return gk_port;
	}
	public void setGk_port(String gk_port) {
		this.gk_port = gk_port;
	}
	public String getGsiftp_port() {
		return gsiftp_port;
	}
	public void setGsiftp_port(String gsiftp_port) {
		this.gsiftp_port = gsiftp_port;
	}	
	public String getSponsor_vo() {
		return sponsor_vo;
	}
	public void setSponsor_vo(String sponsor_vo) {
		this.sponsor_vo = sponsor_vo;
	}
	public String getVdt_version() {
		return vdt_version;
	}
	public void setVdt_version(String vdt_version) {
		this.vdt_version = vdt_version;
	}
	public String getGlobus_loc() {
		return globus_loc;
	}
	public void setGlobus_loc(String globus_loc) {
		this.globus_loc = globus_loc;
	}
	public String getExec_jm() {
		return exec_jm;
	}
	public void setExec_jm(String exec_jm) {
		this.exec_jm = exec_jm;
	}
	public String getUtil_jm() {
		return util_jm;
	}
	public void setUtil_jm(String util_jm) {
		this.util_jm = util_jm;
	}
	public String getGrid_services() {
		return grid_services;
	}
	public void setGrid_services(String grid_services) {
		this.grid_services = grid_services;
	}
	public String getApp_space() {
		return app_space;
	}
	public void setApp_space(String app_space) {
		this.app_space = app_space;
	}
	public String getData_space() {
		return data_space;
	}
	public void setData_space(String data_space) {
		this.data_space = data_space;
	}
	public String getTmp_space() {
		return tmp_space;
	}
	public void setTmp_space(String tmp_space) {
		this.tmp_space = tmp_space;
	}
	
	public String getGsiftp_server() {
		return gsiftp_server;
	}

	public void setGsiftp_server(String gsiftp_server) {
		this.gsiftp_server = gsiftp_server;
	}
}
	
