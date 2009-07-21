package edu.isi.pegasus.planner.catalog.site.impl.myosg.util;
/**
 * This class contains the URL parameter ID's and utility methods for converting user input to param values.
 * @author prasanth
 *
 */
public abstract class URLParamConstants {
	
	public static final int PARAM_SUMMARY_ATTRS_SHOWSERVICE=101;
//	&summary_attrs_showservice=on
	public static final int PARAM_SUMMARY_ATTRS_SHOWRSVSTATUS=102;
//	&summary_attrs_showrsvstatus=on
	public static final int PARAM_SUMMARY_ATTRS_SHOWFQDN=103;
//	&summary_attrs_showfqdn=on
	public static final int PARAM_SUMMARY_ATTRS_SHOWVOMEMBERSHIP=104;
//	&summary_attrs_showvomembership=on
	public static final int PARAM_SUMMARY_ATTRS_SHOWVOOWNERSHIP=105;
//	&summary_attrs_showvoownership=on
	public static final int PARAM_SUMMARY_ATTRS_SHOWENVIRONMNENT=106;
//	summary_attrs_showenv=on&
	public static final int PARAM_GIP_STATUS_ATTRS_SHOWTESTRESULTS=201;
//	&gip_status_attrs_showtestresults=on
	public static final int PARAM_GIP_STATUS_ATTRS_SHOWFQDN=202;
//	&gip_status_attrs_showfqdn=on
	public static final int PARAM_ACCOUNT_TYPE=203;
//	&account_type=cumulative_hours
	public static final int PARAM_CE_ACCOUNT_TYPE=204;
//	&ce_account_type=gip_vo
	public static final int PARAM_SE_ACCOUNT_TYPE=205;
//	&se_account_type=vo_transfer_volume
	public static final int PARAM_START_TYPE=206;
//	&start_type=7daysago
	public static final int PARAM_START_DATE=207;
//	&start_date=05%2F12%2F2009
	public static final int PARAM_END_TYPE=208;
//	&end_type=now
	public static final int PARAM_END_DATE=209;
//	&end_date=05%2F19%2F2009
	public static final int PARAM_RESOURCE_TO_DISPLAY_ALL_RESOURCES = 301;
//	&all_resources=on
	public static final int PARAM_FILTER_GRID_TYPE = 401;
//	&gridtype=on
	public static final int PARAM_FILTER_GRID_TYPE_OPTION = 402;
//	&gridtype_1=on
	public static final int PARAM_FILTER_CURRENT_RSV_STATUS = 403;
//	&status=on
	public static final int PARAM_FILTER_CURRENT_RSV_STATUS_OPTION = 404;
//	&status_1=on
	public static final int PARAM_FILTER_VO_SUPPORT = 405;
//	&vosup=on
	public static final int PARAM_FILTER_VO_SUPPORT_OPTION = 406;
//	&vosup_23=on
	public static final int PARAM_FILTER_ACTIVE_STATUS = 407;
//	&active=on
	public static final int PARAM_FILTER_ACTIVE_STATUS_OPTION = 408;
//	&active_value=1
	public static final int PARAM_FILTER_DISABLE_STATUS = 409;
//	&disable=on
	public static final int PARAM_FILTER_DISABLE_STATUS_OPTION = 410;
//	&disable_value=1
	
	public static final String[][] voNameID = { { "ALICE", "58" },
			{ "ATLAS", "35" }, { "CDF", "1" }, { "CIGI", "2" }, { "CMS", "3" },
			{ "COMPBIOGRID", "4" }, { "DES", "5" }, { "DOSAR", "6" },
			{ "DZERO", "7" }, { "ENGAGE", "8" }, { "FERMILAB", "9" },
			{ "FERMILABACCELERATOR", "51" }, { "FERMILABASTRO", "52" },
			{ "FERMILABCDMS", "49" }, { "FERMILABGRID", "37" },
			{ "FERMILABHYPERCP", "47" }, { "FERMILABKTEV", "50" },
			{ "FERMILABMINERVA", "48" }, { "FERMILABMINIBOONE", "45" },
			{ "FERMILABMINOS", "44" }, { "FERMILABMIPP", "46" },
			{ "FERMILABMU2E", "59" }, { "FERMILABNOVA", "53" },
			{ "FERMILABNUMI", "54" }, { "FERMILABPATRIOT", "55" },
			{ "FERMILABTEST", "43" }, { "FERMILABTHEORY", "56" },
			{ "GEANT4", "12" }, { "GLOW", "13" }, { "GPN", "14" },
			{ "GRASE", "15" }, { "GROW", "17" }, { "I2U2", "19" },
			{ "ICECUBE", "38" }, { "ILC", "20" }, { "JDEM", "57" },
			{ "LIGO", "23" }, { "MARIACHI", "24" }, { "MIS", "25" },
			{ "NANOHUB", "26" }, { "NEBIOGRID", "60" }, { "NWICG", "27" },
			{ "NYSGRID", "28" }, { "OPS", "29" }, { "OSG", "30" },
			{ "OSGEDU", "31" }, { "SBGRID", "32" }, { "SDSS", "33" },
			{ "STAR", "34" } }; // Store as all upper case
	
	public static int getVOID(String name){
		String voCAPS = name.toUpperCase();
		for(int i =0 ;i < voNameID.length;i++){
			if(voNameID[i][0].equals(voCAPS)) {
				return Integer.parseInt(voNameID[i][1]);
			}
		}
		return -1;
	}
	
	public static final String[][] gridTypeID = { { "OSG", "1" },
													{"OSG-ITB","2"}};
	
	public static int getGridTypeID(String type){
		String gridType = type.toUpperCase();
		for(int i =0 ;i < gridTypeID.length;i++){
			if(gridTypeID[i][0].equals(gridType)) {
				return Integer.parseInt(gridTypeID[i][1]);
			}
		}
		return -1;
	}

}
