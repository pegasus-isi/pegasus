package edu.isi.pegasus.planner.catalog.site.impl.myosg.util;


import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.griphyn.cPlanner.common.PegasusProperties;
import org.griphyn.cPlanner.parser.Parser;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;

import edu.isi.pegasus.common.logging.LogManager;
import edu.isi.pegasus.common.logging.LogManagerFactory;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.classes.AbstractSiteCatalogResource;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.classes.MYOSGSiteEnvironmentInfo;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.classes.MYOSGSiteInfo;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.classes.MYOSGSiteResourceInfo;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.classes.MYOSGSiteServiceInfo;
import edu.isi.pegasus.planner.catalog.site.impl.myosg.classes.MYOSGSiteVOOwnershipInfo;

/**
 * This class uses the Xerces SAX2 parser to validate and parse an XML.
 * 
 * @author prasanth
 * 
 */
public class MYOSGSiteCatalogParser extends Parser {

	public static final String RESOURCE_GROUP_TAG = "ResourceGroup";
	public static final String SERVICE_TAG = "Service";
	public static final String RESOURCE_TAG = "Resource";
	public static final String ENV_TAG = "ENV";
	public static final String VO_OWNERSHIP_TAG = "Ownership";
	private int mDepth = 0;

	Stack<AbstractSiteCatalogResource> elementStack = new Stack<AbstractSiteCatalogResource>();
	List<AbstractSiteCatalogResource> siteList = new ArrayList<AbstractSiteCatalogResource>();
	/**
	 * The handle to the log manager.
	 */
	private LogManager mLogger;
	/**
	 * The set of sites that need to be parsed.
	 */
	private Set<String> mSites;

	/**
	 * A boolean indicating whether to load all sites.
	 */
	private boolean mLoadAll;

	/**
	 * The default Constructor.
	 * 
	 * @param sites
	 *            the list of sites to be parsed. * means all.
	 * 
	 */
	
	public MYOSGSiteCatalogParser() {
		this(PegasusProperties.nonSingletonInstance(),null);
	}
	

	public MYOSGSiteCatalogParser(List<String> sites) {
		this(PegasusProperties.nonSingletonInstance(), sites);
	}

	public MYOSGSiteCatalogParser(PegasusProperties properties,
			List<String> sites) {
		super(properties);
		mLogger = LogManagerFactory.loadSingletonInstance();
		if (sites != null) {
			mSites = new HashSet<String>();
			for (Iterator<String> it = sites.iterator(); it.hasNext();) {
				mSites.add(it.next());
			}
			mLoadAll = mSites.contains("*");
		} else {
			mLoadAll = true;
		}

	}

	public void endDocument() {
	}

	 /**
     * The parser is at the end of an element. Triggers the association of
     * the child elements with the appropriate parent elements.
     *
     * @param namespaceURI is the URI of the namespace for the element
     * @param localName is the element name without namespace
     * @param qName is the element name as it appears in the docment
     */  
	public void endElement(String uri, String localName, String name)
			throws SAXException {
		AbstractSiteCatalogResource resource = null;
		mDepth--;
		if (isStackedElement(name)) {
			resource = (AbstractSiteCatalogResource) elementStack.pop();
			if (elementStack.isEmpty()) {
				if (loadSite(resource))
					siteList.add(resource);
			} else {
				AbstractSiteCatalogResource parentResource = (AbstractSiteCatalogResource) elementStack
						.peek();
				parentResource.addChildResource(resource);

			}
		} else {
			if (!elementStack.isEmpty()) {
				resource = (AbstractSiteCatalogResource) elementStack.peek();
				if (resource.getDepth() == mDepth)
					resource.setProperty(name, mTextContent.toString().trim());
			}
		}
		// reinitialize our cdata handler at end of each element
		mTextContent.setLength(0);

	}

	private boolean isStackedElement(String name) {
		if (name.equals(RESOURCE_GROUP_TAG) || name.equals(SERVICE_TAG)
				|| name.equals(RESOURCE_TAG) || name.equals(ENV_TAG) || name.equals(VO_OWNERSHIP_TAG)) {
			return true;
		}
		return false;
	}

	/**
	 * Whether to laod a site or not in the <code>SiteStore</code>
	 * 
	 * @param site
	 *            the <code>SiteCatalogEntry</code> object.
	 * 
	 * @return
	 */
	private boolean loadSite(AbstractSiteCatalogResource site) {
		return (mLoadAll || mSites.contains(site
				.getProperty(MYOSGSiteConstants.SITE_NAME_ID)));
	}

	public String getSchemaLocation() {
		// No Schema supported
		return null;
	}
	 /**
     * This method defines the action to take when the parser begins to parse
     * an element.
     *
     * @param namespaceURI is the URI of the namespace for the element
     * @param localName is the element name without namespace
     * @param qName is the element name as it appears in the docment
     * @param atts has the names and values of all the attributes
     */
	public void startElement(String uri, String local, String name,
			Attributes attrs) throws SAXException {
		mDepth++;
		AbstractSiteCatalogResource resource = null;
		if (name.equals(RESOURCE_GROUP_TAG)) {
			elementStack.push(new MYOSGSiteInfo(mDepth));
		} else if (name.equals(SERVICE_TAG)) {
			elementStack.push(new MYOSGSiteServiceInfo(mDepth));

		} else if (name.equals(RESOURCE_TAG)) {
			elementStack.push(new MYOSGSiteResourceInfo(mDepth));

		}
		else if (name.equals(VO_OWNERSHIP_TAG)) {
			elementStack.push(new MYOSGSiteVOOwnershipInfo(mDepth));
		}
		else if (name.equals(ENV_TAG)) {
			elementStack.push(new MYOSGSiteEnvironmentInfo(mDepth));
		}
	}
        
        
	/**
     * The main method that starts the parsing.
     * 
     * @param file   the XML file to be parsed.
     */
	public void startParser(String file) {
		try {
			mParser.parse(file);
			// sanity check
			if (mDepth != 0) {
				throw new RuntimeException(
						"Invalid stack depth at end of parsing " + mDepth);
			}
		} catch (IOException ioe) {
			mLogger.log("IO Error :" + ioe.getMessage(),
					LogManager.ERROR_MESSAGE_LEVEL);
		} catch (SAXException se) {

			if (mLocator != null) {
				mLogger.log("Error in " + mLocator.getSystemId() + " at line "
						+ mLocator.getLineNumber() + "at column "
						+ mLocator.getColumnNumber() + " :" + se.getMessage(),
						LogManager.ERROR_MESSAGE_LEVEL);
			}
		}

	}

	/**
	 * Returns the site's list
	 * 
	 * @return site's list
	 */
	public List getSites() {
		return this.siteList;
	}

	/**
	 * Returns the number of sites parsed
	 * 
	 * @return number of sites
	 */
	public int getNumberOfSites() {
		return this.siteList.size();
	}

}
