package edu.isi.pegasus.planner.catalog.site.impl.myosg.util;

import java.io.BufferedReader;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
/**
 * Utility class that scrapes content from a website
 * @author prasanth
 *
 */
public class SiteScrapper {
	
	/**
	 * Scarps a web site and stores it in a file
	 * @param urlString URL
	 * @param outputFilePath file name to store the content.
	 */
	 public static void scrapeSite(String urlString, String outputFilePath) {
		URL url = null;
		URLConnection urlConnection = null;
		BufferedReader in = null;
		PrintWriter out = null;
		String line = null;
		try {
			url = new URL(urlString);

			urlConnection = url.openConnection();
			in = new BufferedReader(new InputStreamReader(urlConnection
					.getInputStream()));
			out = new PrintWriter(new FileOutputStream(outputFilePath));
			while ((line = in.readLine()) != null) {
				out.print(line);

			}

		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
				out.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return;
	}

}
