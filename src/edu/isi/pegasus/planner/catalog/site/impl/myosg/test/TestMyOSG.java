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
package edu.isi.pegasus.planner.catalog.site.impl.myosg.test;

import java.io.UnsupportedEncodingException;

import java.net.URLEncoder;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import edu.isi.pegasus.planner.catalog.site.impl.MYOSG;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.DateUtils;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.MYOSGURLGenerator;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.SiteScrapper;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.util.URLParamConstants;

public class TestMyOSG {
	private static final String TMP_FILE_NAME  ="MYOSG_SC.xml";
	private static final String DATE_FORMAT  ="MM/dd/yyyy";
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		Properties paramProperties = new Properties();
		configureProperties(paramProperties);
		String urlString = new MYOSGURLGenerator().getURL(paramProperties);
		new SiteScrapper().scrapeSite(urlString ,TMP_FILE_NAME);
		MYOSG myOSG = new MYOSG();
		Properties properties = new Properties();
		properties.setProperty("file", TMP_FILE_NAME);
		myOSG.connect(properties);
		List <String> sitesList = new ArrayList();
		sitesList.add("*");
		myOSG.load(sitesList);
		System.out.println(myOSG.list().size() +"  " + myOSG.list());

	}
	
	private static void configureProperties(Properties paramProperties){
		paramProperties.setProperty(""+URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWSERVICE,"on");
		paramProperties.setProperty(""+URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWRSVSTATUS,"on");
		paramProperties.setProperty(""+URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWFQDN,"on");
		paramProperties.setProperty(""+URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWVOMEMBERSHIP,"on");
		paramProperties.setProperty(""+URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWVOOWNERSHIP,"on");
		paramProperties.setProperty(""+URLParamConstants.PARAM_SUMMARY_ATTRS_SHOWENVIRONMNENT,"on");
		paramProperties.setProperty(""+URLParamConstants.PARAM_GIP_STATUS_ATTRS_SHOWTESTRESULTS,"on");
		paramProperties.setProperty(""+URLParamConstants.PARAM_GIP_STATUS_ATTRS_SHOWFQDN,"on");
		paramProperties.setProperty(""+URLParamConstants.PARAM_ACCOUNT_TYPE ,"cumulative_hours");
		paramProperties.setProperty(""+URLParamConstants.PARAM_CE_ACCOUNT_TYPE,"gip_vo");
		paramProperties.setProperty(""+URLParamConstants.PARAM_SE_ACCOUNT_TYPE,"vo_transfer_volume");
		paramProperties.setProperty(""+URLParamConstants.PARAM_START_TYPE,"7daysago");
		paramProperties.setProperty(""+URLParamConstants.PARAM_START_DATE,getStartDate());
		paramProperties.setProperty(""+URLParamConstants.PARAM_END_TYPE,"now");
		paramProperties.setProperty(""+URLParamConstants.PARAM_END_DATE,getDateAfter(7));
		paramProperties.setProperty(""+URLParamConstants.PARAM_RESOURCE_TO_DISPLAY_ALL_RESOURCES,"on");
		paramProperties.setProperty(""+URLParamConstants.PARAM_FILTER_GRID_TYPE,"on");
		paramProperties.setProperty(""+URLParamConstants.PARAM_FILTER_GRID_TYPE_OPTION,"1");
		paramProperties.setProperty(""+URLParamConstants.PARAM_FILTER_CURRENT_RSV_STATUS,"on");
		paramProperties.setProperty(""+URLParamConstants.PARAM_FILTER_CURRENT_RSV_STATUS_OPTION,"1");
		paramProperties.setProperty(""+URLParamConstants.PARAM_FILTER_VO_SUPPORT,"on");
		paramProperties.setProperty(""+URLParamConstants.PARAM_FILTER_VO_SUPPORT_OPTION,"23");
		paramProperties.setProperty(""+URLParamConstants.PARAM_FILTER_ACTIVE_STATUS_OPTION,"1");
		paramProperties.setProperty(""+URLParamConstants.PARAM_FILTER_DISABLE_STATUS_OPTION,"1");
	}
	
	private static String getStartDate(){
		String now = null;
		try {
			now =  URLEncoder.encode(DateUtils.now(DATE_FORMAT),"UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return now;
	}
	
	private static String getDateAfter(int days){
		String now = null;
		try {
			now =  URLEncoder.encode(DateUtils.after(days,DATE_FORMAT),"UTF-8");
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return now;

		
		
	}

}
