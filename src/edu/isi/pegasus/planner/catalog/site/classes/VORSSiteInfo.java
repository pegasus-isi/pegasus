package edu.isi.pegasus.planner.catalog.site.classes;

public class VORSSiteInfo {	
	private String shortname; 				//shortname
	private String app_loc;      			//app_loc
	private String data_loc;     			//data_loc
	private String osg_grid;    			//osg_grid
	private String wntmp_loc;              	//wntmp_loc
	private String tmp_loc;      			//tmp_loc
	private String gatekeeper;			 	//gatekeeper
	private String gk_port;     			//gk_port	
	private String gsiftp_port;         	//gsiftp_port	
	private String sponsor_vo;             	//sponsor_vo
	private String vdt_version; 			//vdt_version
	private String globus_loc;              //globus_loc  --if globus_loc is empty then use osg_grid
	private String exec_jm; 				//exec_jm -- transfer
	private String util_jm;                 //util_jm -- compute
	private String grid_services;           //grid_services
	private String app_space;				//app_space -- saved for later use
	private String data_space;				//data_space -- saved for later use
	private String tmp_space;				//tmp_space -- saved for later use
	private VORSVOInfo voInfo;
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
		voInfo.print();
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
	public VORSVOInfo getVoInfo() {
		return voInfo;
	}
	public void setVoInfo(VORSVOInfo voInfo) {
		this.voInfo = voInfo;
	}
}
