package edu.isi.pegasus.planner.catalog.site.impl.myosg.util;

import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

/**
 * This is an utility class which generates the MYOSG url.
 * 
 * @author prasanth
 * 
 */
public class MYOSGURLGenerator {

	private static String BASE_URL = "http://myosg.grid.iu.edu/wizardsummary/xml?";

	/**
	 * Returns the url
	 * 
	 * @param paramProperties
	 *            list of parameterID-value pair
	 * @return URL
	 */
	public String getURL(Properties paramProperties) {
		return BASE_URL + defaultParameters()
				+ getConfigurableParameters(paramProperties);
	}

	/**
	 * Returns the default parameters
	 * 
	 * @return
	 */
	private String defaultParameters() {
		return "datasource=summary";
	}

	/**
	 * Parsers through the list of parameterID-value pair and returns the
	 * parameters name value pair
	 * 
	 * @param paramProperties
	 *            URL's paramID-value parameter pair
	 * @return parameters name value pair
	 */
	private String getConfigurableParameters(Properties paramProperties) {

		Set entrySet = paramProperties.entrySet();
		int paramID = 0;
		String configParam = "";
		String paramValue = "";
		for (Iterator it = entrySet.iterator(); it.hasNext();) {
			Map.Entry entry = (Entry) it.next();
			paramID = Integer.parseInt(((String) entry.getKey()));
			paramValue = (String) entry.getValue();
			configParam += "&" + getParam(paramID, paramValue);
		}
		return configParam;

	}

	/**
	 * Returns the parameter name-value pair
	 * 
	 * @param ID
	 *            parameter ID
	 * @param value
	 *            parameter value
	 * @return parameter name-value pair
	 */
	String getParam(int ID, String value) {
		String param = null;
		switch (ID) {
		case URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWSERVICE:
			param = "summary_attrs_showservice=" + value;
			break;
		case URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWRSVSTATUS:
			param = "summary_attrs_showrsvstatus=" + value;
			break;
		case URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWFQDN:
			param = "summary_attrs_showfqdn=" + value;
			break;
		case URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWVOMEMBERSHIP:
			param = "summary_attrs_showvomembership=" + value;
			break;
		case URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWVOOWNERSHIP:
			param = "summary_attrs_showvoownership=" + value;
			break;
		case URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWENVIRONMNENT:
			param = "summary_attrs_showenv=" + value;
			break;
		case URLParamConstants.PARAM_GIP_STATUS_ATTRS_SHOWTESTRESULTS:
			param = "gip_status_attrs_showtestresults=" + value;
			break;
		case URLParamConstants.PARAM_GIP_STATUS_ATTRS_SHOWFQDN:
			param = "gip_status_attrs_showfqdn=" + value;
			break;
		case URLParamConstants.PARAM_ACCOUNT_TYPE:
			param = "account_type=" + value;
			break;
		case URLParamConstants.PARAM_CE_ACCOUNT_TYPE:
			param = "ce_account_type=" + value;
			break;
		case URLParamConstants.PARAM_SE_ACCOUNT_TYPE:
			param = "se_account_type=" + value;
			break;
		case URLParamConstants.PARAM_START_TYPE:
			param = "start_type=" + value;
			break;
		case URLParamConstants.PARAM_START_DATE:
			param = "start_date=" + value;
			break;
		case URLParamConstants.PARAM_END_TYPE:
			param = "end_type=" + value;
			break;
		case URLParamConstants.PARAM_END_DATE:
			param = "end_date=" + value;
			break;
		case URLParamConstants.PARAM_RESOURCE_TO_DISPLAY_ALL_RESOURCES:
			param = "all_resources=" + value;
			break;
		case URLParamConstants.PARAM_FILTER_GRID_TYPE:
			param = "gridtype=" + value;
			break;
		case URLParamConstants.PARAM_FILTER_GRID_TYPE_OPTION:
			param = "gridtype_" + value + "=" + "on";
			break;
		case URLParamConstants.PARAM_FILTER_CURRENT_RSV_STATUS:
			param = "status=" + value;
			break;
		case URLParamConstants.PARAM_FILTER_CURRENT_RSV_STATUS_OPTION:
			param = "status_" + value + "=" + "on";
			break;
		case URLParamConstants.PARAM_FILTER_VO_SUPPORT:
			param = "vosup=" + value;
			break;
		case URLParamConstants.PARAM_FILTER_VO_SUPPORT_OPTION:
			param = "vosup_" + value + "=" + "on";
			break;
		case URLParamConstants.PARAM_FILTER_ACTIVE_STATUS:
			param = "active=" + value;
			break;
		case URLParamConstants.PARAM_FILTER_ACTIVE_STATUS_OPTION:
			param = "active_value=" + value;
			break;
		case URLParamConstants.PARAM_FILTER_DISABLE_STATUS:
			param = "disable=" + value;
			break;
		case URLParamConstants.PARAM_FILTER_DISABLE_STATUS_OPTION:
			param = "disable_value=" + value;
			break;
		}
		return param;
	}
}
