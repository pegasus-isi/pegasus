/*
 * Copyright 1999-2006 University of Chicago
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.globus.replica.rls;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.globus.gsi.X509Credential;
import org.globus.gsi.gssapi.GlobusGSSCredentialImpl;
import org.globus.gsi.CredentialException;
import org.globus.replica.rls.impl.SimpleRLSConnectionSource;
import org.globus.replica.rls.impl.rpc.SimpleRLSConnection;
import org.globus.util.GlobusURL;

/**
 * This class re-implements the legacy Java/JNI interface using the all Java
 * client interface. It no longer depends on the JNI native library or the
 * native C client interface. This interface is provided for backward
 * compatibility for existing implementations that depend on these interfaces.
 * New users should use the new interfaces. See
 * {@link org.globus.rls.replica.RLSConnection RLSConnection} for more details.
 * 
 * @deprecated Use {@link org.globus.rls.replica.RLSConnection RLSConnection}.
 */
public class RLSClient {

	// CONSTANTS //
	
	private static final String LOCALRLS = "rls://localhost:39281";
	
	public static final int RLS_SUCCESS = 0;

	public static final int RLS_GLOBUSERR = 1;

	public static final int RLS_INVHANDLE = 2;

	public static final int RLS_BADURL = 3;

	public static final int RLS_NOMEMORY = 4;

	public static final int RLS_OVERFLOW = 5;

	public static final int RLS_BADARG = 6;

	public static final int RLS_PERM = 7;

	public static final int RLS_BADMETHOD = 8;

	public static final int RLS_INVSERVER = 9;

	public static final int RLS_MAPPING_NEXIST = 10;

	public static final int RLS_LFN_EXIST = 11;

	public static final int RLS_LFN_NEXIST = 12;

	public static final int RLS_PFN_EXIST = 13;

	public static final int RLS_PFN_NEXIST = 14;

	public static final int RLS_LRC_EXIST = 15;

	public static final int RLS_LRC_NEXIST = 16;

	public static final int RLS_DBERROR = 17;

	public static final int RLS_RLI_EXIST = 18;

	public static final int RLS_RLI_NEXIST = 19;

	public static final int RLS_MAPPING_EXIST = 20;

	public static final int RLS_INV_ATTR_TYPE = 21;

	public static final int RLS_ATTR_EXIST = 22;

	public static final int RLS_ATTR_NEXIST = 23;

	public static final int RLS_INV_OBJ_TYPE = 24;

	public static final int RLS_INV_ATTR_OP = 25;

	public static final int RLS_UNSUPPORTED = 26;

	public static final int RLS_TIMEOUT = 27;

	public static final int RLS_TOO_MANY_CONNECTIONS = 28;

	public static final int RLS_ATTR_VALUE_NEXIST = 29;

	public static final int RLS_ATTR_INUSE = 30;

	public static final int ADMIN_PING = 0;

	public static final int ADMIN_QUIT = 1;

	public static final int ADMIN_SSU = 2;

	public static final int RLIFLAG_BLOOMFILTER = 0x1;
	
	// STATICS //
	
	private static String defaultCertfile = null;
	private static String defaultKeyfile = null;
	private static String defaultProxyfile = null;
	
    private static Log logger = LogFactory.getLog(RLSClient.class.getName());

	
	// FIELDS //
	
	private RLSConnection connection;
	private LocalReplicaCatalog catalog;
	private ReplicaLocationIndex index;
	private Administrative admin;
	private GlobusURL url;

	private LRC lrc;
	private RLI rli;


	// CONSTRUCTORS //
	
	/**
	 * Connect to an RLS server on the local host at the default port.
	 */
	public RLSClient() throws RLSException {
		this(LOCALRLS);
	}

	/**
	 * Connect to an RLS server at the specified url.
	 * 
	 * @param url
	 *            The location of the RLS server. It should be of the form
	 *            rls://host[:port] or rlsn://host[:port]. The latter form is
	 *            used if no authentication is desired.
	 */
	public RLSClient(String url) throws RLSException {
		try {
			if (RLSClient.defaultProxyfile != null
					&& RLSClient.defaultProxyfile.length() > 0) {
				init(url, new GlobusGSSCredentialImpl(
						new X509Credential(RLSClient.defaultProxyfile),
						GSSCredential.INITIATE_ONLY));
			}
			else if (RLSClient.defaultCertfile != null
					&& RLSClient.defaultCertfile.length() > 0
					&& RLSClient.defaultKeyfile != null
					&& RLSClient.defaultKeyfile.length() > 0) {
				init(url, new GlobusGSSCredentialImpl( 
						new X509Credential(RLSClient.defaultProxyfile),
						GSSCredential.INITIATE_ONLY));
			}
			else {
				init(url, null);
			}
		}
		catch (CredentialException e) {
			throw new RLSException(RLSStatusCode.RLS_GLOBUSERR, e);
		}
		catch (GSSException e) {
			throw new RLSException(RLSStatusCode.RLS_GLOBUSERR, e);
		}
	}

	/**
	 * Connect to an RLS server at the specified url, using proxycert for
	 * authentication.
	 * 
	 * @param url
	 *            The location of the RLS server. It should be of the form
	 *            rls://host[:port] or rlsn://host[:port]. The latter form is
	 *            used if no authentication is desired.
	 * @param proxyfile
	 *            Proxy certificate filename to use for authentication.
	 */
	public RLSClient(String url, String proxyfile) throws RLSException {
		try {
			init(url, new GlobusGSSCredentialImpl(
					new X509Credential(proxyfile),
					GSSCredential.INITIATE_ONLY));
		}
		catch (CredentialException e) {
			throw new RLSException(RLSStatusCode.RLS_GLOBUSERR, e);
		}
		catch (GSSException e) {
			throw new RLSException(RLSStatusCode.RLS_GLOBUSERR, e);
		}
	}

	/**
	 * Connect to an RLS server at the specified url, using certfile/key for
	 * authentication.
	 * 
	 * @param url
	 *            The location of the RLS server. It should be of the form
	 *            rls://host[:port] or rlsn://host[:port]. The latter form is
	 *            used if no authentication is desired.
	 * @param certfile
	 *            Certificate file to use for authentication.
	 * @param keyfile
	 *            Key file to use for authentication.
	 */
	public RLSClient(String url, String certfile, String keyfile)
			throws RLSException {
		try {
			init(url, new GlobusGSSCredentialImpl(
					new X509Credential(certfile, keyfile),
					GSSCredential.INITIATE_ONLY));
		}
		catch (CredentialException e) {
			throw new RLSException(RLSStatusCode.RLS_GLOBUSERR, e);
		}
		catch (GSSException e) {
			throw new RLSException(RLSStatusCode.RLS_GLOBUSERR, e);
		}
		catch (IOException e) {
		    throw new RLSException(RLSStatusCode.RLS_GLOBUSERR, e);
		}
	}

	/**
	 * Initializes the RLSClient.
	 * 
	 * @param url String representation of the URL of the RLS server.
	 * @param cred Initiator's credential.
	 * @throws RLSException
	 */
	private void init(String url, GSSCredential cred) throws RLSException {

		// Construct URL
		try {
			this.url = new GlobusURL(url);
		} catch (MalformedURLException e) {
			throw new RLSException(RLSClient.RLS_GLOBUSERR, e.getMessage(), e);
		}

		// Establish connection
		RLSConnectionSource source = new SimpleRLSConnectionSource();
		this.connection = source.connect(this.url, cred);
		
		// Access RLS client interfaces
		this.catalog = this.connection.catalog();
		this.index = this.connection.index();
		this.admin = this.connection.admin();

		// Instantiate legacy interfaces
		this.lrc = new LRC();
		this.rli = new RLI();
	}

	/**
	 * Miscellaneous administrative operations. Most operations require the
	 * ADMIN privilege.
	 * 
	 * @param h
	 *            Handle connected to RLS server.
	 * 
	 * @param cmd
	 *            Command to be sent to RLS server. See
	 *            {@link org.globus.replica.rls.RLSClient#ADMIN_PING admin commands}.
	 */
	public void Admin(int cmd) throws RLSException {
		switch (cmd) {
		case ADMIN_PING:
			this.admin.ping();
			break;
		case ADMIN_QUIT:
			this.admin.quit();
			break;
		case ADMIN_SSU:
			this.admin.update();
			break;
		default:
			throw new RLSException(RLSStatusCode.RLS_BADARG,
					RLSStatusCode.toMessage(RLSStatusCode.RLS_BADARG));
		}
	}

	/**
	 * Set name of cert and key files.
	 */
	public static void Certificate(String certfile, String keyfile) {
		RLSClient.defaultCertfile = certfile;
		RLSClient.defaultKeyfile = keyfile;
	}

	/**
	 * Set name of proxy cert file.
	 */
	public static void ProxyCertificate(String proxyfile) {
		RLSClient.defaultProxyfile = proxyfile;
	}

	/**
	 * Close connection to RLS server.
	 */
	public void Close() throws RLSException {
		this.connection.close();
	}

	/**
	 * Map status code returned by RLS to string.
	 * 
	 * @param rc
	 *            Status code.
	 * 
	 * @return String Error message.
	 */
	public String getErrorMessage(int rc) {
		return RLSStatusCode.toMessage(rc);
	}

	/**
	 * Returns an instance of the LRC.
	 * 
	 * @return Reference to LRC object.
	 */
	public LRC getLRC() {
		return lrc;
	}

	/**
	 * Returns an instance of the RLI.
	 * 
	 * @return Reference to RLI object.
	 */
	public RLI getRLI() {
		return rli;
	}

	/**
	 * Get timeout, in seconds, used in IO calls. In the JNI implementation of
	 * the RLSClient, the default is 30 seconds. However, in the current 
	 * Java-only implementation of the RLSClient, the default is dependent on 
	 * the Java socket implementation. See {@link java.net.Socket}. In order to
	 * maintain backward compatibility with the original method, no exception
	 * is thrown by this method if it fails to get the connection timeout from
	 * the underlying socket. Instead, a message is logged at the ERROR level
	 * and a value of -1 is returned.
	 * 
	 * @return seconds Timeout value or -1 if underlying get timeout failed.
	 */
	public int GetTimeout() {
		try {
			return ((SimpleRLSConnection)this.connection).getTimeout() / 1000;
		}
		catch (Exception e) {
			logger.error("Failed to getTimeout from Socket-based RLSConnection"
					+ " (cause: " + e.getLocalizedMessage() + ")");
			return -1;
		}
	}

	/**
	 * Set timeout value for IO calls. In the JNI implementation of the 
	 * RLSClient, this value applies to all RLS connections, not just the 
	 * current one. However, in the current Java-only implementation of the 
	 * RLSClient, the value applies to this RLS connection only. In order to 
	 * maintain backward compatibility with the original method, no exception
	 * is thrown by this method if it fails to set the connection timeout for
	 * the underlying socket. Instead, a message is logged at the ERROR level.
	 * 
	 * @param seconds Timeout value.
	 */
	public void SetTimeout(int seconds) {
		try {
			((SimpleRLSConnection)this.connection).setTimeout(seconds * 1000);
		}
		catch (Exception e) {
			logger.error("Failed to setTimeout for Socket-based RLSConnection"
					+ " (cause: " + e.getLocalizedMessage() + ")");
		}
	}

	/**
	 * Return URL of RLS server.
	 */
	public String GetURL() {
		return url.getURL();
	}

	/**
	 * Add an attribute to an object in the LRC database.
	 * 
	 * @param key
	 *            Logical or Physical File Name (LFN or PFN) that identifies
	 *            object attribute should be added to.
	 * 
	 * @param attr
	 *            Attribute to be added to object. See
	 *            {@link org.globus.replica.rls.RLSAttribute#LRC_LFN object types}.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#attributeAdd
	 *             getLRC().attributeAdd()}
	 */
	public void LRCAttrAdd(String key, RLSAttribute attr) throws RLSException {
		lrc.attributeAdd(key, attr);
	}

	/**
	 * Define new attribute in LRC database.
	 * 
	 * @param name
	 *            Name of attribute.
	 * 
	 * @param objtype
	 *            Object (LFN or PFN) type that attribute applies to. See
	 *            {@link org.globus.replica.rls.RLSAttribute#LRC_LFN object types}.
	 * 
	 * @param valtype
	 *            Type of attribute value. See
	 *            {@link org.globus.replica.rls.RLSAttribute#DATE attribute types}.
	 * 
	 * @deprecated Use
	 *             {@link org.globus.replica.rls.RLSClient.LRC#attributeCreate
	 *             getLRC().attributeCreate()}
	 */
	public void LRCAttrCreate(String name, int objtype, int valtype)
			throws RLSException {
		lrc.attributeCreate(name, objtype, valtype);
	}

	/**
	 * Undefine attribute in LRC database.
	 * 
	 * @param name
	 *            Name of attribute.
	 * 
	 * @param objtype
	 *            Object (LFN or PFN) type that attribute applies to. See
	 *            {@link org.globus.replica.rls.RLSAttribute#LRC_LFN object types}.
	 * 
	 * @param clearvalues
	 *            If true then any any values for this attribute are first
	 *            removed from the objects they're associated with. If false and
	 *            any values exist then an exception is thrown.
	 * 
	 * @deprecated Use
	 *             {@link org.globus.replica.rls.RLSClient.LRC#attributeDelete
	 *             getLRC().attributeDelete()}
	 */
	public void LRCAttrDelete(String name, int objtype, boolean clearvalues)
			throws RLSException {
		lrc.attributeDelete(name, objtype, clearvalues);
	}

	/**
	 * Return definitions of attributes in LRC database.
	 * 
	 * @param name
	 *            Name of attribute. If name is null all attributes of the
	 *            specified objtype are returned.
	 * 
	 * @param objtype
	 *            Object (LFN or PFN) type that attribute applies to. See
	 *            {@link org.globus.replica.rls.RLSAttribute#LRC_LFN object types}.
	 * 
	 * @return attr_list Any attribute definitions found will be returned as a
	 *         list of {@link org.globus.replica.rls.RLSAttribute RLSAttribute}
	 *         objects.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#attributeGet
	 *             getLRC().attributeGet()}
	 */
	public ArrayList LRCAttrGet(String name, int objtype) throws RLSException {
		return lrc.attributeGet(name, objtype);
	}

	/**
	 * Modify an attribute value.
	 * 
	 * @param key
	 *            Name of object (LFN or PFN).
	 * 
	 * @param attr
	 *            Attribute to be modified.
	 * 
	 * @deprecated Use
	 *             {@link org.globus.replica.rls.RLSClient.LRC#attributeModify
	 *             getLRC().attributeModify()}
	 */
	public void LRCAttrModify(String key, RLSAttribute attr)
			throws RLSException {
		lrc.attributeModify(key, attr);
	}

	/**
	 * Remove an attribute from an object (LFN or PFN) in the LRC database.
	 * 
	 * @param key
	 *            Name of object (LFN or PFN).
	 * 
	 * @param attr
	 *            Attribute to be removed.
	 * 
	 * @deprecated Use
	 *             {@link org.globus.replica.rls.RLSClient.LRC#attributeRemove
	 *             getLRC().attributeRemove()}
	 */
	public void LRCAttrRemove(String key, RLSAttribute attr)
			throws RLSException {
		lrc.attributeRemove(key, attr);
	}

	/**
	 * Search for objects (LFNs or PFNs) in a LRC database that have the
	 * specified attribute whose value matches a boolean expression.
	 * 
	 * @param name
	 *            Name of attribute.
	 * 
	 * @param objtype
	 *            Object (LFN or PFN) type that attribute applies to.
	 * 
	 * @param op
	 *            Operator to be used in searching for values. See
	 *            {@link org.globus.replica.rls.RLSAttribute#OPALL operators}.
	 * 
	 * @param operand1
	 *            First operand in boolean expression.
	 * 
	 * @param operand2
	 *            Second operand in boolean expression, only used when op is
	 *            {@link org.globus.replica.rls.RLSAttribute#OPBTW RLSAttribute.OPBTW}.
	 * 
	 * @param offlim
	 *            Offset and limit used to retrieve results incrementally. Use
	 *            null, or 0,0, to retrieve all results.
	 * 
	 * @return attr_obj_list Any objects with the specified attribute will be
	 *         returned, with the attribute value, in a list of
	 *         {@link org.globus.replica.rls.RLSAttributeObject RLSAttributeObject}
	 *         objects.
	 * 
	 * @deprecated Use
	 *             {@link org.globus.replica.rls.RLSClient.LRC#attributeSearch
	 *             getLRC().attributeSearch()}
	 */
	public ArrayList LRCAttrSearch(String name, int objtype, int op,
			RLSAttribute op1, RLSAttribute op2, RLSOffsetLimit offlim)
			throws RLSException {
		return lrc.attributeSearch(name, objtype, op, op1, op2, offlim);
	}

	/**
	 * Return attributes in LRC database for specified object (LFN or PFN).
	 * 
	 * @param key
	 *            Logical or Physical File Name (LFN or PFN) that identifies
	 *            object attributes should be retrieved for.
	 * 
	 * @param name
	 *            Name of attribute to retrieve. If null all attributes for key,
	 *            objtype are returned.
	 * 
	 * @param objtype
	 *            Object (LFN or PFN) type that attribute applies to.
	 * 
	 * @return attr_list Any attributes found will be returned in a list of
	 *         {@link org.globus.replica.rls.RLSAttribute RLSAttribute} objects.
	 * 
	 * @deprecated Use
	 *             {@link org.globus.replica.rls.RLSClient.LRC#attributeValueGet
	 *             getLRC().attributeValueGet()}
	 */
	public ArrayList LRCAttrValueGet(String key, String name, int objtype)
			throws RLSException {
		return lrc.attributeValueGet(key, name, objtype);
	}

	/**
	 * Add mapping to PFN to an existing LFN.
	 * 
	 * @param lfn
	 *            LFN to add pfn mapping to, should already exist.
	 * 
	 * @param pfn
	 *            PFN that lfn should map to.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#add
	 *             getLRC().add()}
	 */
	public void LRCAdd(String lfn, String pfn) throws RLSException {
		lrc.add(lfn, pfn);
	}

	/**
	 * Remove all lfns, pfns, mappings and attribute values from LRC database.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#clear
	 *             getLRC().clear()}
	 */
	public void LRCClear() throws RLSException {
		lrc.clear();
	}

	/**
	 * Create new mapping to PFN from LFN.
	 * 
	 * @param lfn
	 *            LFN to add pfn mapping to, should not exist.
	 * 
	 * @param pfn
	 *            PFN that lfn should map to.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#create
	 *             getLRC().create()}
	 */
	public void LRCCreate(String lfn, String pfn) throws RLSException {
		lrc.create(lfn, pfn);
	}

	/**
	 * Delete mapping from LRC database.
	 * 
	 * @param lfn
	 *            LFN to remove mapping from.
	 * 
	 * @param pfn
	 *            PFN that LFN maps to be removed.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#delete
	 *             getLRC().delete()}
	 */
	public void LRCDelete(String lfn, String pfn) throws RLSException {
		lrc.delete(lfn, pfn);
	}

	/**
	 * Test if object (LFN or PFN) exists.
	 * 
	 * @param key
	 *            Name of object.
	 * 
	 * @param objtype
	 *            Type of object.
	 * 
	 * @return boolean True if object exists, else false.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#exists
	 *             getLRC().exists()}
	 */
	public boolean LRCExists(String key, int objtype) throws RLSException {
		return lrc.exists(key, objtype);
	}

	/**
	 * Return LFNs mapped to PFN in the LRC database.
	 * 
	 * @param pfn
	 *            PFN to search for.
	 * 
	 * @param offlim
	 *            Offset and limit used to retrieve results incrementally. Use
	 *            null, or 0,0, to retrieve all results.
	 * 
	 * @return str2_list List of {@link org.globus.replica.rls.RLSString2
	 *         RLSString2} objects containing matching lfn,pfn mappings.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#getLFN
	 *             getLRC().getLFN()}
	 */
	public ArrayList LRCGetLFN(String pfn, RLSOffsetLimit offlim)
			throws RLSException {
		return lrc.getLFN(pfn, offlim);
	}

	/**
	 * Return all LFNs mapped to PFN in the LRC database.
	 * 
	 * @param pfn
	 *            PFN to search for.
	 * 
	 * @return str2_list List of {@link org.globus.replica.rls.RLSString2
	 *         rls.RLSString2} objects containing matching lfn,pfn mappings.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#getLFN
	 *             getLRC().getLFN()}
	 */
	public ArrayList LRCGetLFN(String pfn) throws RLSException {
		return lrc.getLFN(pfn);
	}

	/**
	 * Return LFNs mapped to PFNs matching pfnpat in the LRC database.
	 * 
	 * @param pfnpat
	 *            PFN pattern to search for.
	 * 
	 * @param offlim
	 *            Offset and limit used to retrieve results incrementally. Use
	 *            null, or 0,0, to retrieve all results.
	 * 
	 * @return str2_list List of
	 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2} objects
	 *         containing matching lfn,pfn mappings.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#getLFNWC
	 *             getLRC().getLFNWC()}
	 */
	public ArrayList LRCGetLFNWC(String pfnpat, RLSOffsetLimit offlim)
			throws RLSException {
		return lrc.getLFNWC(pfnpat, offlim);
	}

	/**
	 * Return all LFNs mapped to PFNs matching pfnpat in the LRC database.
	 * 
	 * @param pfnpat
	 *            PFN pattern to search for.
	 * 
	 * @return str2_list List of
	 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2} objects
	 *         containing matching lfn,pfn mappings.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#getLFNWC
	 *             getLRC().getLFNWC()}
	 */
	public ArrayList LRCGetLFNWC(String pfnpat) throws RLSException {
		return lrc.getLFNWC(pfnpat);
	}

	/**
	 * Return PFNs mapped to LFN in the LRC database.
	 * 
	 * @param lfn
	 *            LFN to search for.
	 * 
	 * @param offlim
	 *            Offset and limit used to retrieve results incrementally. Use
	 *            null, or 0,0, to retrieve all results.
	 * 
	 * @return str2_list List of
	 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2} objects
	 *         containing matching lfn,pfn mappings.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#getPFN
	 *             getLRC().gePLFN()}
	 */
	public ArrayList LRCGetPFN(String lfn, RLSOffsetLimit offlim)
			throws RLSException {
		return lrc.getPFN(lfn, offlim);
	}

	/**
	 * Return all PFNs mapped to LFN in the LRC database.
	 * 
	 * @param lfn
	 *            LFN to search for.
	 * 
	 * @return str2_list List of
	 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2} objects
	 *         containing matching lfn,pfn mappings.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#getPFN
	 *             getLRC().gePLFN()}
	 */
	public ArrayList LRCGetPFN(String lfn) throws RLSException {
		return lrc.getPFN(lfn);
	}

	/**
	 * Return PFNs mapped to LFNs matching lfnpat in the LRC database.
	 * 
	 * @param lfnpat
	 *            LFN pattern to search for.
	 * 
	 * @param offlim
	 *            Offset and limit used to retrieve results incrementally. Use
	 *            null, or 0,0, to retrieve all results.
	 * 
	 * @return str2_list List of
	 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2} objects
	 *         containing matching lfn,pfn mappings.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#getPFNWC
	 *             getLRC().gePLFNWC()}
	 */
	public ArrayList LRCGetPFNWC(String lfnpat, RLSOffsetLimit offlim)
			throws RLSException {
		return lrc.getPFNWC(lfnpat, offlim);
	}

	/**
	 * Return all PFNs mapped to LFNs matching lfnpat in the LRC database.
	 * 
	 * @param lfnpat
	 *            LFN pattern to search for.
	 * 
	 * @return str2_list List of
	 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2} objects
	 *         containing matching lfn,pfn mappings.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#getPFNWC
	 *             getLRC().gePLFNWC()}
	 */
	public ArrayList LRCGetPFNWC(String lfnpat) throws RLSException {
		return lrc.getPFNWC(lfnpat);
	}

	/**
	 * Test if mapping exists in LRC.
	 * 
	 * @param lfn
	 *            Name of LFN.
	 * 
	 * @param pfn
	 *            Name of PFN.
	 * 
	 * @return boolean True if mapping exists, else false.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#mappingExists
	 *             getLRC().mappingExists()}
	 */
	public boolean LRCMappingExists(String lfn, String pfn) throws RLSException {
		return lrc.mappingExists(lfn, pfn);
	}

	/**
	 * LRC servers send information about LFNs in their database to the the list
	 * of RLI servers in the database, added with the following function.
	 * Updates may be partitioned amongst multiple RLIs by specifying one or
	 * more patterns for an RLI.
	 * 
	 * @param rliurl
	 *            URL of RLI server that LRC should send updates to.
	 * 
	 * @param flags
	 *            Should be zero or
	 *            {@link org.globus.replica.rls.RLSClient#RLIFLAG_BLOOMFILTER RLSClient.RLIFLAG_BLOOMFILTER}
	 *            if the RLI should be updated via Bloom filters.
	 * 
	 * @param pattern
	 *            If not NULL used to filter which LFNs are sent to rli_url.
	 *            Standard Unix wildcard characters (*, ?) may be used to do
	 *            wildcard matches. Patterns are ignored if Bloom filters are
	 *            used for updates.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#rliAdd
	 *             getLRC().rliAdd()}
	 */
	public void LRCRLIAdd(String rliurl, int flags, String pattern)
			throws RLSException {
		lrc.rliAdd(rliurl, flags, pattern);
	}

	/**
	 * Delete an entry from the LRC rli/partition tables.
	 * 
	 * @param rliurl
	 *            URL of RLI server to remove from LRC partition table.
	 * 
	 * @param pattern
	 *            If not null then only the specific rli_url/pattern is removed,
	 *            else all partition information for rli_url is removed.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#rliDelete
	 *             getLRC().rliDelete()}
	 */
	public void LRCRLIDelete(String rliurl, String pattern) throws RLSException {
		lrc.rliDelete(rliurl, pattern);
	}

	/**
	 * Get RLI update partitions from LRC server.
	 * 
	 * @param rliurl
	 *            If not NULL identifies RLI that partition data will be
	 *            retrieved for. If null then all RLIs are retrieved.
	 * 
	 * @param pattern
	 *            If not NULL returns only partitions with matching patterns,
	 *            otherwise all patterns are retrieved.
	 * 
	 * @return str2_list List of
	 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2} objects,
	 *         containing the RLI URLs and patterns (if any) updated by the LRC.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#rliGetPart
	 *             getLRC().rliGetPart()}
	 */
	public ArrayList LRCRLIGetPart(String rliurl, String pattern)
			throws RLSException {
		return lrc.rliGetPart(rliurl, pattern);
	}

	/**
	 * Get info about RLI server updated by an LRC server.
	 * 
	 * @param rliurl
	 *            URL of RLI server to retrieve info for.
	 * 
	 * @return info Data about the RLI server will be returned in an
	 *         {@link org.globus.replica.rls.RLSRLIInfo RLSRLIInfo} object.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#rliInfo
	 *             getLRC().rliInfo()}
	 */
	public RLSRLIInfo LRCRLIInfo(String rliurl) throws RLSException {
		return lrc.rliInfo(rliurl);
	}

	/**
	 * Return URLs of RLIs that LRC sends updates to.
	 * 
	 * @return A list of RLSRLIInfo objects containing the RLI URLs updated by
	 *         this LRC.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.LRC#rliList
	 *             getLRC().rliList()}
	 */
	public ArrayList LRCRLIList() throws RLSException {
		return lrc.rliList();
	}

	/**
	 * Return LRCs mapped to LFN in the LRC database.
	 * 
	 * @param lfn
	 *            LFN to search for.
	 * 
	 * @param offlim
	 *            Offset and limit used to retrieve results incrementally. Use
	 *            null, or 0,0, to retrieve all results.
	 * 
	 * @return str2_list List of
	 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2} objects
	 *         containing matching lfn,lrc mappings.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.RLI#getLRC
	 *             getRLI().getLRC()}
	 */
	public ArrayList RLIGetLRC(String lfn, RLSOffsetLimit offlim)
			throws RLSException {
		return rli.getLRC(lfn, offlim);
	}

	/**
	 * Return all LRCs mapped to LFNs matching lfnpat in the LRC database.
	 * 
	 * @param lfnpat
	 *            LFN pattern to search for.
	 * 
	 * @return str2_list List of
	 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2} objects
	 *         containing matching lfn,lrc mappings.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.RLI#getLRC
	 *             getRLI().getLRC()}
	 */
	public ArrayList RLIGetLRC(String lfn) throws RLSException {
		return rli.getLRC(lfn);
	}

	/**
	 * Return LRCs mapped to LFNs matching lfnpat in the LRC database.
	 * 
	 * @param lfnpat
	 *            LFN pattern to search for.
	 * 
	 * @param offlim
	 *            Offset and limit used to retrieve results incrementally. Use
	 *            null, or 0,0, to retrieve all results.
	 * 
	 * @return str2_list List of
	 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2} objects
	 *         containing matching lfn,lrc mappings.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.RLI#getLRCWC
	 *             getRLI().getLRCWC()}
	 */
	public ArrayList RLIGetLRCWC(String lfnpat, RLSOffsetLimit offlim)
			throws RLSException {
		return rli.getLRCWC(lfnpat, offlim);
	}

	/**
	 * Return all LRCs mapped to lfns matching lfnpat in the LRC database.
	 * 
	 * @param lfnpat
	 *            LFN pattern to search for.
	 * 
	 * @return str2_list List of
	 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2} objects
	 *         containing matching lfn,lrc mappings.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.RLI#getLRCWC
	 *             getRLI().getLRCWC()}
	 */
	public ArrayList RLIGetLRCWC(String lfnpat) throws RLSException {
		return rli.getLRCWC(lfnpat);
	}

	/**
	 * Test if object (LFN or LRC) exists.
	 * 
	 * @param key
	 *            Name of object.
	 * 
	 * @param objtype
	 *            Type of object.
	 * 
	 * @return boolean True if object exists, else false.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.RLI#exists
	 *             getRLI().exists()}
	 */
	public boolean RLIExists(String key, int objtype) throws RLSException {
		return rli.exists(key, objtype);
	}

	/**
	 * Return URLs of LRCs that update this RLI.
	 * 
	 * @return A list of RLSLRCInfo objects containing the RLI URLs updated by
	 *         this LRC.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.RLI#lrcList
	 *             getRLI().lrcList()}
	 */
	public ArrayList RLILRCList() throws RLSException {
		return rli.lrcList();
	}

	/**
	 * Test if mapping exists in RLI.
	 * 
	 * @param lfn
	 *            Name of LFN.
	 * 
	 * @param lrc
	 *            Name of LRC.
	 * 
	 * @return boolean True if mapping exists, else false.
	 * 
	 * @deprecated Use {@link org.globus.replica.rls.RLSClient.RLI#mappingExists
	 *             getRLI().mappingExists()}
	 */
	public boolean RLIMappingExists(String lfn, String lrc) throws RLSException {
		return rli.mappingExists(lfn, lrc);
	}

	/**
	 * Return server configuration.
	 * 
	 * @param option
	 *            Configuration option to retreive. If null all options are
	 *            returned
	 * 
	 * @return str2_list List of {@link org.globus.replica.rls.RLSString2
	 *         RLSString2} objects containing configuration option names and
	 *         values.
	 */
	public ArrayList GetConfiguration(String option) throws RLSException {
		List raw = this.admin.getConfiguration(option);
		ArrayList results = new ArrayList(raw.size());
		Iterator iter = raw.iterator();
		while (iter.hasNext()) {
			ConfigurationOption opt = (ConfigurationOption) iter.next();
			results.add(new RLSString2(opt.getName(), opt.getValue()));
		}
		raw.clear();
		return results;
	}

	/**
	 * Set server configuration.
	 * 
	 * @param option
	 *            Configuration option to set.
	 * @param value
	 *            Value to set.
	 * @return <code>RLS_SUCCESS</code> if successful.
	 * @throws Throws
	 *             an exception if the RLS server returns an error code.
	 */
	public int SetConfiguration(String option, String value)
			throws RLSException {
		this.admin.setConfiguration(option, value);
		return RLSStatusCode.RLS_SUCCESS;
	}

	/**
	 * Return stats about an RLS server.
	 * 
	 * @return RLSStats.
	 * 
	 */
	public RLSStats Stats() throws RLSException {
		return this.admin.stats();
	}

	/**
	 * Make call to system(), hack to allow me to invoke globus-url-copy for the
	 * sc2002 demo.
	 * 
	 * @return int system() result.
	 * @deprecated No longer supported
	 */
	static public int OSSystem(String cmd) {
		// RLS client no longer supports this operation
		throw new UnsupportedOperationException("This method is currently unsupported");
	}

	// INNER CLASSES //

	/**
	 * This class implements the LRC interface to an RLS server.
	 * 
	 * @version 2.0.3
	 */
	public class LRC {

		/**
		 * Add mapping to PFN to an existing LFN.
		 * 
		 * @param lfn
		 *            LFN to add pfn mapping to, should already exist.
		 * 
		 * @param pfn
		 *            PFN that lfn should map to.
		 */
		public void add(String lfn, String pfn) throws RLSException {
			if (lfn == null || pfn == null) {
				throw new RLSException(RLSStatusCode.RLS_BADARG,
						"Invalid arguments: null");
			}
			List mappings = new ArrayList(1);
			mappings.add(new Mapping(lfn, pfn));
			List results = catalog.addMappings(mappings);
			if (results != null && !results.isEmpty()) {
				MappingResult result = (MappingResult) results.get(0);
				results.clear();
				throw new RLSException(result.getRC(), 
						RLSStatusCode.toMessage(result.getRC()));
			}
		}

		/**
		 * Bulk add mappings to PFNs to existing LFNs.
		 * 
		 * @param str2list
		 *            <code>ArrayList</code> of <code>RLSString2</code>
		 *            elements, where <code>s1</code> is the LFN to add PFN
		 *            mapping to, and <code>s2</code> is the PFN that LFN
		 *            should map to. The LFN should already exist.
		 * 
		 * @return List of failed updates. ArrayList elements will be of type
		 *         <code>RLSString2Bulk</code>.
		 */
		public ArrayList addBulk(ArrayList str2list) throws RLSException {
			List mappings = RLSClient.convertRLSString2ToMappings(str2list);
			List results = catalog.addMappings(mappings);
			ArrayList s2blist = 
				RLSClient.convertMappingResultsToRLSString2Bulk(results, true);
			if (results != null )
				results.clear();
			return s2blist;
		}

		/**
		 * Add an attribute to an object in the LRC database.
		 * 
		 * @param key
		 *            Logical or Physical File Name (LFN or PFN) that identifies
		 *            object attribute should be added to.
		 * 
		 * @param attr
		 *            Attribute to be added to object. See
		 *            {@link org.globus.replica.rls.RLSAttribute#LRC_LFN object types}.
		 */
		public void attributeAdd(String key, RLSAttribute attr)
				throws RLSException {
			List attrs = new ArrayList(1);
			attrs.add(new RLSAttributeObject(attr, key));
			List results = catalog.addAttributes(attrs);
			if (results != null && !results.isEmpty()) {
				AttributeResult result = (AttributeResult) results.get(0);
				results.clear();
				throw new RLSException(result.getRC(), 
						RLSStatusCode.toMessage(result.getRC()));
			}
		}

		/**
		 * Bulk add attributes.
		 * 
		 * @param attr_obj_list
		 *            Object and attributes to add to them. Elements must be of
		 *            type <code>RLSAttributeObject</code>.
		 * 
		 * @return List of failed updates. ArrayList elements will be of type
		 *         RLSString2Bulk.
		 */
		public ArrayList attributeAddBulk(ArrayList attr_obj_list)
				throws RLSException {
			List results = catalog.addAttributes(attr_obj_list);
			ArrayList s2blist = 
				RLSClient.convertAttributeResultsToRLSString2Bulk(results);
			results.clear();
			return s2blist;
		}

		/**
		 * Define new attribute in LRC database.
		 * 
		 * @param name
		 *            Name of attribute.
		 * 
		 * @param objtype
		 *            Object (LFN or PFN) type that attribute applies to. See
		 *            {@link org.globus.replica.rls.RLSAttribute#LRC_LFN object types}.
		 * 
		 * @param valtype
		 *            Type of attribute value. See
		 *            {@link org.globus.replica.rls.RLSAttribute#DATE attribute types}.
		 */
		public void attributeCreate(String name, int objtype, int valtype)
				throws RLSException {
			List attrs = new ArrayList(1);
			attrs.add(new RLSAttribute(name, objtype, valtype));
			List results = catalog.defineAttributes(attrs);
			if (results != null && !results.isEmpty()) {
				AttributeResult result = (AttributeResult) results.get(0);
				results.clear();
				throw new RLSException(result.getRC(),
						RLSStatusCode.toMessage(result.getRC()));
			}
		}

		/**
		 * Undefine attribute in LRC database.
		 * 
		 * @param name
		 *            Name of attribute.
		 * 
		 * @param objtype
		 *            Object (LFN or PFN) type that attribute applies to. See
		 *            {@link org.globus.replica.rls.RLSAttribute#LRC_LFN object types}.
		 * 
		 * @param clearvalues
		 *            If true then any any values for this attribute are first
		 *            removed from the objects they're associated with. If false
		 *            and any values exist then an exception is thrown.
		 */
		public void attributeDelete(String name, int objtype,
				boolean clearvalues) throws RLSException {
			List attrs = new ArrayList(1);
			attrs.add(new RLSAttribute(name, objtype, 0));
			List results = catalog.undefineAttributes(attrs, clearvalues);
			if (results != null && !results.isEmpty()) {
				AttributeResult result = (AttributeResult) results.get(0);
				results.clear();
				throw new RLSException(result.getRC(), 
						RLSStatusCode.toMessage(result.getRC()));
			}
		}

		/**
		 * Return definitions of attributes in LRC database.
		 * 
		 * @param name
		 *            Name of attribute. If name is null all attributes of the
		 *            specified objtype are returned.
		 * 
		 * @param objtype
		 *            Object (LFN or PFN) type that attribute applies to. See
		 *            {@link org.globus.replica.rls.RLSAttribute#LRC_LFN object types}.
		 * 
		 * @return attr_list Any attribute definitions found will be returned as
		 *         a list of
		 *         {@link org.globus.replica.rls.RLSAttribute RLSAttribute}
		 *         objects.
		 */
		public ArrayList attributeGet(String name, int objtype)
				throws RLSException {
			RLSAttribute attr = new RLSAttribute(name, objtype, -1);
			SimpleCatalogQuery qry = new SimpleCatalogQuery(
					SimpleCatalogQuery.queryAttributeDefinitions,
					attr, null);
			Results results = catalog.query(qry);
			if (results.getRC() != RLSStatusCode.RLS_SUCCESS) {
				throw new RLSException(results.getRC(), 
						RLSStatusCode.toMessage(results.getRC()));
			}
			return new ArrayList(results.getBatch());
		}

		/**
		 * Modify an attribute value.
		 * 
		 * @param key
		 *            Name of object (LFN or PFN).
		 * 
		 * @param attr
		 *            Attribute to be modified.
		 */
		public void attributeModify(String key, RLSAttribute attr)
				throws RLSException {
			List attrs = new ArrayList(1);
			attrs.add(new RLSAttributeObject(attr, key));
			List results = catalog.modifyAttributes(attrs);
			if (results != null && !results.isEmpty()) {
				AttributeResult result = (AttributeResult) results.get(0);
				results.clear();
				throw new RLSException(result.getRC(), 
						RLSStatusCode.toMessage(result.getRC()));
			}
		}

		/**
		 * Remove an attribute from an object (LFN or PFN) in the LRC database.
		 * 
		 * @param key
		 *            Name of object (LFN or PFN).
		 * 
		 * @param attr
		 *            Attribute to be removed.
		 */
		public void attributeRemove(String key, RLSAttribute attr)
				throws RLSException {
			List attrs = new ArrayList(1);
			attrs.add(new RLSAttributeObject(attr, key));
			List results = catalog.removeAttributes(attrs);
			if (results != null && !results.isEmpty()) {
				AttributeResult result = (AttributeResult) results.get(0);
				results.clear();
				throw new RLSException(result.getRC(), 
						RLSStatusCode.toMessage(result.getRC()));
			}
		}

		/**
		 * Bulk remove attributes.
		 * 
		 * @param attr_obj_list
		 *            Object and attributes to remove from them. Elements must
		 *            be of type <code>RLSAttributeObject<code>.
		 *
		 * @return List of failed updates.  ArrayList elements will be of
		 *   type RLSString2Bulk.
		 */
		public ArrayList attributeRemoveBulk(ArrayList attr_obj_list)
				throws RLSException {
			List results = catalog.removeAttributes(attr_obj_list);
			ArrayList s2blist = 
				RLSClient.convertAttributeResultsToRLSString2Bulk(results);
			results.clear();
			return s2blist;
		}

		/**
		 * Search for objects (LFNs or PFNs) in a LRC database that have the
		 * specified attribute whose value matches a boolean expression.
		 * 
		 * @param name
		 *            Name of attribute.
		 * 
		 * @param objtype
		 *            Object (LFN or PFN) type that attribute applies to.
		 * 
		 * @param op
		 *            Operator to be used in searching for values. See
		 *            {@link org.globus.replica.rls.RLSAttribute#OPALL operators}.
		 * 
		 * @param operand1
		 *            First operand in boolean expression.
		 * 
		 * @param operand2
		 *            Second operand in boolean expression, only used when op is
		 *            {@link org.globus.replica.rls.RLSAttribute#OPBTW RLSAttribute.OPBTW}.
		 * 
		 * @param offlim
		 *            Offset and limit used to retrieve results incrementally.
		 *            Use null, or 0,0, to retrieve all results.
		 * 
		 * @return attr_obj_list Any objects with the specified attribute will
		 *         be returned, with the attribute value, in a list of
		 *         {@link org.globus.replica.rls.RLSAttributeObject RLSAttributeObject}
		 *         objects.
		 */
		public ArrayList attributeSearch(String name, int objtype, int op,
				RLSAttribute op1, RLSAttribute op2, RLSOffsetLimit offlim)
				throws RLSException {
			AttributeSearch qry = new AttributeSearch(name, objtype, op, op1,
					op2, offlim);
			Results results = catalog.query(qry);
			if (results.getRC() != RLSStatusCode.RLS_SUCCESS) {
				throw new RLSException(results.getRC(), 
						RLSStatusCode.toMessage(results.getRC()));
			}
			return new ArrayList(results.getBatch());
		}

		/**
		 * Return attributes in LRC database for specified object (LFN or PFN).
		 * 
		 * @param key
		 *            Logical or Physical File Name (LFN or PFN) that identifies
		 *            object attributes should be retrieved for.
		 * 
		 * @param name
		 *            Name of attribute to retrieve. If null all attributes for
		 *            key, objtype are returned.
		 * 
		 * @param objtype
		 *            Object (LFN or PFN) type that attribute applies to.
		 * 
		 * @return attr_list Any attributes found will be returned in a list of
		 *         {@link org.globus.replica.rls.RLSAttribute RLSAttribute}
		 *         objects.
		 */
		public ArrayList attributeValueGet(String key, String name, int objtype)
				throws RLSException {
			// Convert null name to empty string
			if (name == null)
				name = "";
			List keylist = new ArrayList(1);
			keylist.add(key);
			BatchCatalogQuery qry = new BatchCatalogQuery(keylist, name,
					new Integer(objtype));
			Results results = catalog.query(qry);
			if (results.getRC() != RLSStatusCode.RLS_SUCCESS) {
				throw new RLSException(results.getRC(), 
						RLSStatusCode.toMessage(results.getRC()));
			}
			ArrayList output = RLSClient.convertRLAttributeObjectsToRLSAttributes(
					results.getBatch());
			// If output is null, then return empty list to conform with old api
			return (output != null) ? output : new java.util.ArrayList();
		}

		/**
		 * Bulk return attributes in LRC database for specified objects (LFNs or
		 * PFNs).
		 * 
		 * @param keylist
		 *            Logical or Physical File Name (LFN or PFN) that identify
		 *            objects that attributes should be retrieved for. Elements
		 *            of type <code>String</code>.
		 * 
		 * @param name
		 *            Name of attribute to retrieve. If null all attributes for
		 *            key, objtype are returned.
		 * 
		 * @param objtype
		 *            Object (LFN or PFN) type that attribute applies to.
		 * 
		 * @return attr_obj_list Any attributes found will be returned in a list
		 *         of {@link org.globus.replica.rls.RLSAttributeObject
		 *         RLSAttributeObject} objects.
		 */
		public ArrayList attributeValueGetBulk(ArrayList keylist, String name,
				int objtype) throws RLSException {
			// Convert null name to empty string
			if (name == null)
				name = "";
			BatchCatalogQuery qry = new BatchCatalogQuery(keylist, name,
					new Integer(objtype));
			Results results = catalog.query(qry);
			if (results.getRC() != RLSStatusCode.RLS_SUCCESS) {
				throw new RLSException(results.getRC(), 
						RLSStatusCode.toMessage(results.getRC()));
			}
			return new ArrayList(results.getBatch());
		}

		/**
		 * Remove all lfns, pfns, mappings and attribute values from LRC
		 * database.
		 * 
		 * @deprecated This method is no longer supported. Invoking it will
		 *             result in an exception.
		 */
		public void clear() throws RLSException {
			throw new RLSException(
					RLSStatusCode.RLS_BADMETHOD,
					"The clear method is no longer supported. The clear operation "
							+ "cannot gaurantee consistency should only be used from the RLS "
							+ "client in a development environment.");
		}

		/**
		 * Create new mapping to PFN from LFN.
		 * 
		 * @param lfn
		 *            LFN to add pfn mapping to, should not exist.
		 * 
		 * @param pfn
		 *            PFN that lfn should map to.
		 */
		public void create(String lfn, String pfn) throws RLSException {
			if (lfn == null || pfn == null) {
				throw new RLSException(RLSStatusCode.RLS_BADARG,
						"Invalid arguments: null");
			}
			ArrayList mappings = new ArrayList(1);
			mappings.add(new Mapping(lfn, pfn));
			List results = catalog.createMappings(mappings);
			if (results != null && !results.isEmpty()) {
				MappingResult result = (MappingResult) results.get(0);
				results.clear();
				throw new RLSException(result.getRC(), 
						RLSStatusCode.toMessage(result.getRC()));
			}
		}

		/**
		 * Bulk create LFN,PFN mappings.
		 * 
		 * @param str2list
		 *            List of <code>RLSString2</code> elements, where
		 *            <code>s1</code> is the LFN and <code>s2</code> is the
		 *            PFN.
		 * 
		 * @return List of failed updates. ArrayList elements will be of type
		 *         RLSString2Bulk.
		 */
		public ArrayList createBulk(ArrayList str2list) throws RLSException {
			List mappings = RLSClient.convertRLSString2ToMappings(str2list);
			List results = catalog.createMappings(mappings);
			ArrayList s2blist = 
				RLSClient.convertMappingResultsToRLSString2Bulk(results, true);
			if (results != null )
				results.clear();
			return s2blist;
		}

		/**
		 * Delete mapping from LRC database.
		 * 
		 * @param lfn
		 *            LFN to remove mapping from.
		 * 
		 * @param pfn
		 *            PFN that LFN maps to be removed.
		 */
		public void delete(String lfn, String pfn) throws RLSException {
			if (lfn == null || pfn == null) {
				throw new RLSException(RLSStatusCode.RLS_BADARG,
						"Invalid arguments: null");
			}
			ArrayList mappings = new ArrayList(1);
			mappings.add(new Mapping(lfn, pfn));
			List results = catalog.deleteMappings(mappings);
			if (results != null && !results.isEmpty()) {
				MappingResult result = (MappingResult) results.get(0);
				results.clear();
				throw new RLSException(result.getRC(), 
						RLSStatusCode.toMessage(result.getRC()));
			}
		}

		/**
		 * Bulk delete LFN,PFN mappings.
		 * 
		 * @param str2list
		 *            List of <code>RLSString2</code> elements, where
		 *            <code>s1</code> is the LFN and <code>s2</code> is the
		 *            PFN.
		 * 
		 * @return List of failed updates. ArrayList elements will be of type
		 *         RLSString2Bulk.
		 */
		public ArrayList deleteBulk(ArrayList str2list) throws RLSException {
			List mappings = RLSClient.convertRLSString2ToMappings(str2list);
			List results = catalog.deleteMappings(mappings);
			ArrayList s2blist = 
				RLSClient.convertMappingResultsToRLSString2Bulk(results, true);
			if (results != null )
				results.clear();
			return s2blist;
		}

		/**
		 * Test if object (LFN or PFN) exists.
		 * 
		 * @param key
		 *            Name of object.
		 * 
		 * @param objtype
		 *            Type of object.
		 * 
		 * @return boolean True if object exists, else false.
		 */
		public boolean exists(String key, int objtype) throws RLSException {
			List batch = new ArrayList(1);
			batch.add(key);
			CatalogExistenceQuery qry = new CatalogExistenceQuery(
					CatalogExistenceQuery.objectExists, batch,
					new Integer(objtype));
			Results results = catalog.query(qry);
			if (results.getRC() == RLSStatusCode.RLS_SUCCESS)
				return true;
			else
				return false;
		}

		/**
		 * Bulk test if object (LFN or PFN) exists.
		 * 
		 * @param strlist
		 *            List of objects (LFN or PFN) to test for. Elements are of
		 *            type <code>String </code>.
		 * 
		 * @param objtype
		 *            Type of object.
		 * 
		 * @return List of query results. ArrayList elements will be of type
		 *         RLSString2Bulk.
		 */
		public ArrayList existsBulk(ArrayList strlist, int objtype)
				throws RLSException {
			CatalogExistenceQuery qry = new CatalogExistenceQuery(
					CatalogExistenceQuery.objectExists, strlist,
					new Integer(objtype));
			Results results = catalog.query(qry);
			return RLSClient.convertMappingExistsToRLSString2Bulk(
					results.getBatch());
		}

		/**
		 * Return LFNs mapped to PFN in the LRC database.
		 * 
		 * @param pfn
		 *            PFN to search for.
		 * 
		 * @param offlim
		 *            Offset and limit used to retrieve results incrementally.
		 *            Use null, or 0,0, to retrieve all results.
		 * 
		 * @return str2_list List of {@link org.globus.replica.rls.RLSString2
		 *         RLSString2} objects containing matching lfn,pfn mappings.
		 */
		public ArrayList getLFN(String pfn, RLSOffsetLimit offlim)
				throws RLSException {
			SimpleCatalogQuery qry = new SimpleCatalogQuery(
					SimpleCatalogQuery.queryMappingsByTargetName, pfn, offlim);
			Results results = catalog.query(qry);
			return RLSClient.convertMappingResultsToRLSString2(
					results.getBatch());
		}

		/**
		 * Return all LFNs mapped to PFN in the LRC database.
		 * 
		 * @param pfn
		 *            PFN to search for.
		 * 
		 * @return str2_list List of {@link org.globus.replica.rls.RLSString2
		 *         rls.RLSString2} objects containing matching lfn,pfn mappings.
		 */
		public ArrayList getLFN(String pfn) throws RLSException {
			SimpleCatalogQuery qry = new SimpleCatalogQuery(
					SimpleCatalogQuery.queryMappingsByTargetName, pfn, null);
			Results results = catalog.query(qry);
			return RLSClient.convertMappingResultsToRLSString2(results
					.getBatch());
		}

		/**
		 * Bulk query of PFNs.
		 * 
		 * @param strlist
		 *            List of PFNs to search for. Elements of type
		 *            <code>String</code>.
		 * 
		 * @return List of query results. ArrayList elements will be of type
		 *         RLSString2Bulk.
		 */
		public ArrayList getLFNBulk(ArrayList strlist) throws RLSException {
			BatchCatalogQuery qry = new BatchCatalogQuery(
					BatchCatalogQuery.mappingQueryByTargetNames, strlist);
			Results results = catalog.query(qry);
			return RLSClient.convertMappingResultsToRLSString2Bulk(
					results.getBatch(), false);
		}

		/**
		 * Return LFNs mapped to PFNs matching pfnpat in the LRC database.
		 * 
		 * @param pfnpat
		 *            PFN pattern to search for.
		 * 
		 * @param offlim
		 *            Offset and limit used to retrieve results incrementally.
		 *            Use null, or 0,0, to retrieve all results.
		 * 
		 * @return str2_list List of
		 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2}
		 *         objects containing matching lfn,pfn mappings.
		 */
		public ArrayList getLFNWC(String pfnpat, RLSOffsetLimit offlim)
				throws RLSException {
			SimpleCatalogQuery qry = new SimpleCatalogQuery(
					SimpleCatalogQuery.queryMappingsByTargetNamePattern, pfnpat,
					offlim);
			Results results = catalog.query(qry);
			return RLSClient.convertMappingResultsToRLSString2(results
					.getBatch());
		}

		/**
		 * Return all LFNs mapped to PFNs matching pfnpat in the LRC database.
		 * 
		 * @param pfnpat
		 *            PFN pattern to search for.
		 * 
		 * @return str2_list List of
		 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2}
		 *         objects containing matching lfn,pfn mappings.
		 */
		public ArrayList getLFNWC(String pfnpat) throws RLSException {
			SimpleCatalogQuery qry = new SimpleCatalogQuery(
					SimpleCatalogQuery.queryMappingsByTargetNamePattern, pfnpat,
					null);
			Results results = catalog.query(qry);
			return RLSClient.convertMappingResultsToRLSString2(
					results.getBatch());
		}

		/**
		 * Return PFNs mapped to LFN in the LRC database.
		 * 
		 * @param lfn
		 *            LFN to search for.
		 * 
		 * @param offlim
		 *            Offset and limit used to retrieve results incrementally.
		 *            Use null, or 0,0, to retrieve all results.
		 * 
		 * @return str2_list List of
		 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2}
		 *         objects containing matching lfn,pfn mappings.
		 */
		public ArrayList getPFN(String lfn, RLSOffsetLimit offlim)
				throws RLSException {
			SimpleCatalogQuery qry = new SimpleCatalogQuery(
					SimpleCatalogQuery.queryMappingsByLogicalName, lfn, offlim);
			Results results = catalog.query(qry);
			return RLSClient.convertMappingResultsToRLSString2(
					results.getBatch());
		}

		/**
		 * Bulk query of LFNs.
		 * 
		 * @param strlist
		 *            List of LFNs to search for. Elements of type
		 *            <code>String</code>.
		 * 
		 * @return List of query results. ArrayList elements will be of type
		 *         RLSString2Bulk.
		 */
		public ArrayList getPFNBulk(ArrayList strlist) throws RLSException {
			BatchCatalogQuery qry = new BatchCatalogQuery(
					BatchCatalogQuery.mappingQueryByLogicalNames, strlist);
			Results results = catalog.query(qry);
			return RLSClient.convertMappingResultsToRLSString2Bulk(
					results.getBatch(), false);
		}

		/**
		 * Return all PFNs mapped to LFN in the LRC database.
		 * 
		 * @param lfn
		 *            LFN to search for.
		 * 
		 * @return str2_list List of
		 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2}
		 *         objects containing matching lfn,pfn mappings.
		 */
		public ArrayList getPFN(String lfn) throws RLSException {
			SimpleCatalogQuery qry = new SimpleCatalogQuery(
					SimpleCatalogQuery.queryMappingsByLogicalName, lfn, null);
			Results results = catalog.query(qry);
			return RLSClient.convertMappingResultsToRLSString2(
					results.getBatch());
		}

		/**
		 * Return PFNs mapped to LFNs matching lfnpat in the LRC database.
		 * 
		 * @param lfnpat
		 *            LFN pattern to search for.
		 * 
		 * @param offlim
		 *            Offset and limit used to retrieve results incrementally.
		 *            Use null, or 0,0, to retrieve all results.
		 * 
		 * @return str2_list List of
		 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2}
		 *         objects containing matching lfn,pfn mappings.
		 */
		public ArrayList getPFNWC(String lfnpat, RLSOffsetLimit offlim)
				throws RLSException {
			SimpleCatalogQuery qry = new SimpleCatalogQuery(
					SimpleCatalogQuery.queryMappingsByLogicalNamePattern,
					lfnpat, offlim);
			Results results = catalog.query(qry);
			return RLSClient.convertMappingResultsToRLSString2(
					results.getBatch());
		}

		/**
		 * Return all PFNs mapped to LFNs matching lfnpat in the LRC database.
		 * 
		 * @param lfnpat
		 *            LFN pattern to search for.
		 * 
		 * @return str2_list List of
		 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2}
		 *         objects containing matching lfn,pfn mappings.
		 */
		public ArrayList getPFNWC(String lfnpat) throws RLSException {
			SimpleCatalogQuery qry = new SimpleCatalogQuery(
					SimpleCatalogQuery.queryMappingsByLogicalNamePattern,
					lfnpat, null);
			Results results = catalog.query(qry);
			return RLSClient.convertMappingResultsToRLSString2(
					results.getBatch());
		}

		/**
		 * Test if mapping exists in LRC.
		 * 
		 * @param lfn
		 *            Name of LFN.
		 * 
		 * @param pfn
		 *            Name of PFN.
		 * 
		 * @return boolean True if mapping exists, else false.
		 */
		public boolean mappingExists(String lfn, String pfn)
				throws RLSException {
			List batch = new ArrayList(1);
			batch.add(new Mapping(lfn, pfn));
			CatalogExistenceQuery qry = new CatalogExistenceQuery(
					CatalogExistenceQuery.mappingExists, batch);
			Results results = catalog.query(qry);
			if (results.getRC() != RLSStatusCode.RLS_SUCCESS)
				throw new RLSException(results.getRC());
			else {
				MappingResult r = (MappingResult) results.getBatch().get(0);
				return (r.getRC() == RLSStatusCode.RLS_SUCCESS);
			}
		}

		/**
		 * Rename an LFN.
		 * 
		 * @param from
		 *            Name of existing LFN.
		 * 
		 * @param to
		 *            New name for LFN. Name must not exist.
		 */
		public void renameLFN(String from, String to) throws RLSException {
			if (from == null || to == null) {
				throw new RLSException(RLSStatusCode.RLS_BADARG,
						"Invalid arguments: null");
			}
			List renames = new ArrayList(1);
			renames.add(new Rename(from, to));
			List results = catalog.renameLogicalNames(renames);
			if (results != null && !results.isEmpty()) {
				RenameResult result = (RenameResult) results.get(0);
				results.clear();
				throw new RLSException(result.getRC(), 
						RLSStatusCode.toMessage(result.getRC()));
			}
		}

		/**
		 * Bulk rename LFNs.
		 * 
		 * @param str2list
		 *            List of LFNs to rename. Elements of type
		 *            <code>RLSString2</code>, where <code>s1</code> is the
		 *            name of an existing LFN, and <code>s2</code> is the new
		 *            name for the LFN.
		 * 
		 * @return List of failed updates. ArrayList elements will be of type
		 *         <code>RLSString2Bulk</code>.
		 */
		public ArrayList renameLFNBulk(ArrayList str2list) throws RLSException {
			List renames = RLSClient.convertRLSString2ToRenames(str2list);
			List results = catalog.renameLogicalNames(renames);
			ArrayList s2blist = RLSClient
					.convertRenameResultsToRLSString2Bulk(results);
			if (results != null)
				results.clear();
			return s2blist;
		}

		/**
		 * Rename an PFN.
		 * 
		 * @param from
		 *            Name of existing PFN.
		 * 
		 * @param to
		 *            New name for PFN. Name must not exist.
		 */
		public void renamePFN(String from, String to) throws RLSException {
			if (from == null || to == null) {
				throw new RLSException(RLSStatusCode.RLS_BADARG,
						"Invalid arguments: null");
			}
			List renames = new ArrayList(1);
			renames.add(new Rename(from, to));
			List results = catalog.renameTargetNames(renames);
			if (results != null && !results.isEmpty()) {
				RenameResult result = (RenameResult) results.get(0);
				results.clear();
				throw new RLSException(result.getRC(), 
						RLSStatusCode.toMessage(result.getRC()));
			}
		}

		/**
		 * Bulk rename PFNs.
		 * 
		 * @param str2list
		 *            List of PFNs to rename. Elements of type
		 *            <code>RLSString2</code>, where <code>s1</code> is the
		 *            name of an existing PFN, and <code>s2</code> is the new
		 *            name for the PFN.
		 * 
		 * @return List of failed updates. ArrayList elements will be of type
		 *         <code>RLSString2Bulk</code>.
		 */
		public ArrayList renamePFNBulk(ArrayList str2list) throws RLSException {
			List renames = RLSClient.convertRLSString2ToRenames(str2list);
			List results = catalog.renameTargetNames(renames);
			ArrayList s2blist = RLSClient
					.convertRenameResultsToRLSString2Bulk(results);
			if (results != null)
				results.clear();
			return s2blist;
		}

		/**
		 * LRC servers send information about LFNs in their database to the the
		 * list of RLI servers in the database, added with the following
		 * function. Updates may be partitioned amongst multiple RLIs by
		 * specifying one or more patterns for an RLI.
		 * 
		 * @param rliurl
		 *            URL of RLI server that LRC should send updates to.
		 * 
		 * @param flags
		 *            Should be zero or
		 *            {@link org.globus.replica.rls.RLSClient#RLIFLAG_BLOOMFILTER RLSClient.RLIFLAG_BLOOMFILTER}
		 *            if the RLI should be updated via Bloom filters.
		 * 
		 * @param pattern
		 *            If not NULL used to filter which LFNs are sent to rli_url.
		 *            Standard Unix wildcard characters (*, ?) may be used to do
		 *            wildcard matches. Patterns are ignored if Bloom filters
		 *            are used for updates.
		 */
		public void rliAdd(String rliurl, int flags, String pattern)
				throws RLSException {
			admin.addUpdate(new RLIUpdate(rliurl, flags, pattern));
		}

		/**
		 * Delete an entry from the LRC rli/partition tables.
		 * 
		 * @param rliurl
		 *            URL of RLI server to remove from LRC partition table.
		 * 
		 * @param pattern
		 *            If not null then only the specific rli_url/pattern is
		 *            removed, else all partition information for rli_url is
		 *            removed.
		 */
		public void rliDelete(String rliurl, String pattern)
				throws RLSException {
			admin.deleteUpdate(new RLIUpdate(rliurl, pattern));
		}

		/**
		 * Get RLI update partitions from LRC server.
		 * 
		 * @param rliurl
		 *            If not NULL identifies RLI that partition data will be
		 *            retrieved for. If null then all RLIs are retrieved.
		 * 
		 * @param pattern
		 *            If not NULL returns only partitions with matching
		 *            patterns, otherwise all patterns are retrieved.
		 * 
		 * @return str2_list List of
		 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2}
		 *         objects, containing the RLI URLs and patterns (if any)
		 *         updated by the LRC.
		 */
		public ArrayList rliGetPart(String rliurl, String pattern)
				throws RLSException {
			List results = admin.listUpdatePartitions(
					new RLIUpdate(rliurl, pattern));
			return convertRLIUpdateToRLSString2(results);
		}

		/**
		 * Get info about RLI server updated by an LRC server.
		 * 
		 * @param rliurl
		 *            URL of RLI server to retrieve info for.
		 * 
		 * @return info Data about the RLI server will be returned in an
		 *         {@link org.globus.replica.rls.RLSRLIInfo RLSRLIInfo} object.
		 */
		public RLSRLIInfo rliInfo(String rliurl) throws RLSException {
			return admin.findUpdate(rliurl);
		}

		/**
		 * Return URLs of RLIs that LRC sends updates to.
		 * 
		 * @return A list of RLSRLIInfo objects containing the RLI URLs updated
		 *         by this LRC.
		 */
		public ArrayList rliList() throws RLSException {
			return new ArrayList(admin.listUpdates());
		}

	} // LRC [END]

	/**
	 * This class implements the RLI interface to an RLS server.
	 * 
	 * @version 2.0.3
	 */
	public class RLI {

		/**
		 * Return LRCs mapped to LFN in the LRC database.
		 * 
		 * @param lfn
		 *            LFN to search for.
		 * 
		 * @param offlim
		 *            Offset and limit used to retrieve results incrementally.
		 *            Use null, or 0,0, to retrieve all results.
		 * 
		 * @return str2_list List of
		 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2}
		 *         objects containing matching lfn,lrc mappings.
		 */
		public ArrayList getLRC(String lfn, RLSOffsetLimit offlim)
				throws RLSException {
			SimpleIndexQuery qry = new SimpleIndexQuery(
					SimpleIndexQuery.queryMappingsByLogicalName, lfn, offlim);
			Results results = index.query(qry);
			return RLSClient.convertIndexMappingResultsToRLSString2(
					results.getBatch());
		}

		/**
		 * Return all LRCs mapped to LFNs matching lfn in the LRC database.
		 * 
		 * @param lfnpat
		 *            LFN to search for.
		 * 
		 * @return str2_list List of
		 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2}
		 *         objects containing matching lfn,lrc mappings.
		 */
		public ArrayList getLRC(String lfn) throws RLSException {
			SimpleIndexQuery qry = new SimpleIndexQuery(
					SimpleIndexQuery.queryMappingsByLogicalName, lfn, null);
			Results results = index.query(qry);
			return RLSClient.convertIndexMappingResultsToRLSString2(
					results.getBatch());
		}

		/**
		 * Bulk query of LFNs.
		 * 
		 * @param strlist
		 *            List of LFNs to search for. Elements of type
		 *            <code>String</code>.
		 * 
		 * @return List of query results. ArrayList elements will be of type
		 *         RLSString2Bulk.
		 */
		public ArrayList getLRCBulk(ArrayList strlist) throws RLSException {
			BatchIndexQuery qry = new BatchIndexQuery(
					BatchIndexQuery.queryMappingsByLogicalNames, strlist);
			Results results = index.query(qry);
			return RLSClient.convertIndexMappingResultsToRLSString2Bulk(
					results.getBatch());
		}

		/**
		 * Return LRCs mapped to LFNs matching lfnpat in the LRC database.
		 * 
		 * @param lfnpat
		 *            LFN pattern to search for.
		 * 
		 * @param offlim
		 *            Offset and limit used to retrieve results incrementally.
		 *            Use null, or 0,0, to retrieve all results.
		 * 
		 * @return str2_list List of
		 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2}
		 *         objects containing matching lfn,lrc mappings.
		 */
		public ArrayList getLRCWC(String lfnpat, RLSOffsetLimit offlim)
				throws RLSException {
			SimpleIndexQuery qry = new SimpleIndexQuery(
					SimpleIndexQuery.queryMappingsByLogicalNamePattern, lfnpat,
					offlim);
			Results results = index.query(qry);
			return RLSClient.convertIndexMappingResultsToRLSString2(results
					.getBatch());
		}

		/**
		 * Return all LRCs mapped to lfns matching lfnpat in the LRC database.
		 * 
		 * @param lfnpat
		 *            LFN pattern to search for.
		 * 
		 * @return str2_list List of
		 *         {@link org.globus.replica.rls.RLSString2 rls.RLSString2}
		 *         objects containing matching lfn,lrc mappings.
		 */
		public ArrayList getLRCWC(String lfnpat) throws RLSException {
			SimpleIndexQuery qry = new SimpleIndexQuery(
					SimpleIndexQuery.queryMappingsByLogicalNamePattern, lfnpat,
					null);
			Results results = index.query(qry);
			return RLSClient.convertIndexMappingResultsToRLSString2(results
					.getBatch());
		}

		/**
		 * Test if object (LFN or LRC) exists.
		 * 
		 * @param key
		 *            Name of object.
		 * 
		 * @param objtype
		 *            Type of object.
		 * 
		 * @return boolean True if object exists, else false.
		 */
		public boolean exists(String key, int objtype) throws RLSException {
			List batch = new ArrayList(1);
			batch.add(key);
			IndexExistenceQuery qry = new IndexExistenceQuery(
					IndexExistenceQuery.objectExists, batch, new Integer(
							objtype));
			Results results = index.query(qry);
			if (results.getRC() == RLSStatusCode.RLS_SUCCESS)
				return true;
			else
				return false;
		}

		/**
		 * Bulk test if object (LFN or LRC) exists.
		 * 
		 * @param strlist
		 *            List of objects (LFN or LRC) to test for. Elements of type
		 *            <code>String</code>.
		 * 
		 * @param objtype
		 *            Type of object.
		 * 
		 * @return List of query results. ArrayList elements will be of type
		 *         <code>RLSString2Bulk</code>.
		 */
		public ArrayList existsBulk(ArrayList strlist, int objtype)
				throws RLSException {
			IndexExistenceQuery qry = new IndexExistenceQuery(
					IndexExistenceQuery.objectExists, strlist,
					new Integer(objtype));
			Results results = index.query(qry);
			return RLSClient.convertIndexMappingExistsToRLSString2Bulk(results
					.getBatch());
		}

		/**
		 * Return URLs of LRCs that update this RLI.
		 * 
		 * @return A list of RLSLRCInfo objects containing the RLI URLs updated
		 *         by this LRC.
		 */
		public ArrayList lrcList() throws RLSException {
			return new ArrayList(admin.updatedBy());
		}

		/**
		 * Test if mapping exists in RLI.
		 * 
		 * @param lfn
		 *            Name of LFN.
		 * 
		 * @param lrc
		 *            Name of LRC.
		 * 
		 * @return boolean True if mapping exists, else false.
		 */
		public boolean mappingExists(String lfn, String lrc)
				throws RLSException {
			List batch = new ArrayList(1);
			batch.add(new IndexMapping(lfn, lrc));
			IndexExistenceQuery qry = new IndexExistenceQuery(
					IndexExistenceQuery.mappingExists, batch);
			Results results = index.query(qry);
			if (results.getRC() != RLSStatusCode.RLS_SUCCESS)
				throw new RLSException(results.getRC());
			else {
				IndexMappingResult r = 
					(IndexMappingResult) results.getBatch().get(0);
				return (r.getRC() == RLSStatusCode.RLS_SUCCESS);
			}
		}

		/**
		 * Return URLs of RLIs that RLI sends updates to.
		 * 
		 * @return A list of RLSRLIInfo objects containing the RLI URLs updated
		 *         by this RLI.
		 */
		public ArrayList rliList() throws RLSException {
			return new ArrayList(admin.listRliToRliUpdates());
		}

	}

	// UTILITY METHODS //

	/**
	 * Converts ArrayList of RLSString2 elements to List of Mapping elements.
	 * 
	 * @param str2list RLSString2 list.
	 * @throws RLSException Indicates bad argument list.
	 */
	static final List convertRLSString2ToMappings(List str2list) 
	throws RLSException {
		if (str2list == null || str2list.isEmpty() ||
				!(str2list.get(0) instanceof RLSString2)) {
			throw new RLSException(RLSStatusCode.RLS_BADARG, "Bad input list:" +
					" Expected non-empty list of RLSString2 elements.");
		}
		List mappings = new ArrayList(str2list.size());
		for (int i = 0; i < str2list.size(); i++) {
			RLSString2 str2 = (RLSString2) str2list.get(i);
			mappings.add(new Mapping(str2.s1, str2.s2));
		}
		return mappings;
	}

	/**
	 * Converts List of MappingResult elements to ArrayList of RLSString2Bulk
	 * elements.
	 * 
	 * @param results
	 *            MappingResult list.
	 * @return RLSString2Bulk list.
	 */
	static final ArrayList convertMappingResultsToRLSString2Bulk(List results, boolean getboth) {
		if (results == null)
			return null;

		ArrayList s2blist = new ArrayList(results.size());
		Iterator iter = results.iterator();
		while (iter.hasNext()) {
			MappingResult result = (MappingResult) iter.next();
			int rc = result.getRC();
			String s1 = null;
			String s2 = null;
			if (rc == RLSStatusCode.RLS_SUCCESS || getboth) {
				s1 = result.getLogical();
				s2 = result.getTarget();
			}
			else {
				s1 = result.getLogical();
				if (s1 == null)
					s1 = result.getTarget();
			}
			s2blist.add(new RLSString2Bulk(rc,s1,s2));
		}
		return s2blist;
	}

	/**
	 * Converts List of MappingResult elements to ArrayList of RLSString
	 * elements.
	 * 
	 * @param results MappingResult list.
	 * @return RLSString2 list.
	 */
	static final ArrayList convertMappingResultsToRLSString2(List results) {
		if (results == null)
			return null;

		ArrayList s2list = new ArrayList(results.size());
		Iterator iter = results.iterator();
		while (iter.hasNext()) {
			MappingResult result = (MappingResult) iter.next();
			s2list.add(new RLSString2(result.getLogical(),result.getTarget()));
		}
		return s2list;
	}

	/**
	 * Converts List of MappingResult elements to ArrayList of RLSString2Bulk
	 * elements. This conversion is good for existence queries which return only
	 * a single value among the logical/target pairs.
	 * 
	 * @param results
	 *            MappingResult list.
	 * @return RLSString2Bulk list.
	 */
	static final ArrayList convertMappingExistsToRLSString2Bulk(List results) {
		if (results == null)
			return null;

		ArrayList s2blist = new ArrayList(results.size());
		Iterator iter = results.iterator();
		while (iter.hasNext()) {
			MappingResult result = (MappingResult) iter.next();
			String str = result.getLogical();
			if (str == null)
				str = result.getTarget();
			s2blist.add(new RLSString2Bulk(result.getRC(), str, null));
		}
		return s2blist;
	}

	/**
	 * Converts List of IndexMappingResult elements to ArrayList of RLSString2Bulk
	 * elements.
	 * 
	 * @param results
	 *            IndexMappingResult list.
	 * @return RLSString2Bulk list.
	 */
	static final ArrayList convertIndexMappingResultsToRLSString2Bulk(List results) {
		if (results == null)
			return null;

		ArrayList s2blist = new ArrayList(results.size());
		Iterator iter = results.iterator();
		while (iter.hasNext()) {
			IndexMappingResult result = (IndexMappingResult) iter.next();
			int rc = result.getRC();
			String s1 = null;
			String s2 = null;
			if (rc == RLSStatusCode.RLS_SUCCESS) {
				s1 = result.getLogical();
				s2 = result.getCatalog();
			}
			else {
				s1 = result.getLogical();
				if (s1 == null)
					s1 = result.getCatalog();
			}
			s2blist.add(new RLSString2Bulk(rc,s1,s2));
		}
		return s2blist;
	}

	/**
	 * Converts List of IndexMappingResult elements to ArrayList of RLSString2
	 * elements.
	 * 
	 * @param results IndexMappingResult list.
	 * @return RLSString2 list.
	 */
	static final ArrayList convertIndexMappingResultsToRLSString2(List results) {
		if (results == null)
			return null;

		ArrayList s2list = new ArrayList(results.size());
		Iterator iter = results.iterator();
		while (iter.hasNext()) {
			IndexMappingResult result = (IndexMappingResult) iter.next();
			s2list.add(new RLSString2(result.getLogical(),
					result.getCatalog()));
		}
		return s2list;
	}

	/**
	 * Converts List of IndexMappingResult elements to ArrayList of RLSString2Bulk
	 * elements. This conversion is good for existence queries which return only
	 * a single value among the logical/catalog pairs.
	 * 
	 * @param results
	 *            MappingResult list.
	 * @return RLSString2Bulk list.
	 */
	static final ArrayList convertIndexMappingExistsToRLSString2Bulk(List results) {
		if (results == null)
			return null;

		ArrayList s2blist = new ArrayList(results.size());
		Iterator iter = results.iterator();
		while (iter.hasNext()) {
			IndexMappingResult result = (IndexMappingResult) iter.next();
			String str = result.getLogical();
			if (str == null)
				str = result.getCatalog();
			s2blist.add(new RLSString2Bulk(result.getRC(), str, null));
		}
		return s2blist;
	}

	/**
	 * Converts List of AttributeResult elements to ArrayList of RLSString2Bulk
	 * elements.
	 * 
	 * @param results
	 *            AttributeResult list.
	 * @return RLSString2Bulk list.
	 */
	static final ArrayList convertAttributeResultsToRLSString2Bulk(List results) {
		if (results == null)
			return null;

		ArrayList s2blist = new ArrayList(results.size());
		Iterator iter = results.iterator();
		while (iter.hasNext()) {
			AttributeResult result = (AttributeResult) iter.next();
			s2blist.add(new RLSString2Bulk(result.getRC(), result.getKey()));
		}
		return s2blist;
	}

	/**
	 * Converts List of RLSString2 elements to List of Rename elements.
	 * 
	 * @param str2list
	 *            RLSString2 list.
	 */
	static final List convertRLSString2ToRenames(List str2list)
	throws RLSException {
		if (str2list == null || str2list.isEmpty() ||
				!(str2list.get(0) instanceof RLSString2)) {
			throw new RLSException(RLSStatusCode.RLS_BADARG, "Bad input list:" +
					" Expected non-empty list of RLSString2 elements.");
		}
		List renames = new ArrayList(str2list.size());
		for (int i = 0; i < str2list.size(); i++) {
			RLSString2 str2 = (RLSString2) str2list.get(i);
			renames.add(new Rename(str2.s1, str2.s2));
		}
		return renames;
	}

	/**
	 * Converts List of RenameResult elements to ArrayList of RLSString2Bulk
	 * elements.
	 * 
	 * @param results
	 *            RenameResult list.
	 * @return RLSString2Bulk list.
	 */
	static final ArrayList convertRenameResultsToRLSString2Bulk(List results) {
		if (results == null)
			return null;

		ArrayList s2blist = new ArrayList(results.size());
		Iterator iter = results.iterator();
		while (iter.hasNext()) {
			RenameResult result = (RenameResult) iter.next();
			s2blist.add(new RLSString2Bulk(result.getRC(), result.getFrom(),
					result.getTo()));
		}
		return s2blist;
	}

	/**
	 * Converts List of RLSAttributeObject elements to ArrayList of RLSAttribute
	 * elements. Also, it will free up the RLSAttributeObject elements fields
	 * and clear the input list.
	 * 
	 * @param results
	 *            RLSAttributeObject list.
	 * @return RLSAttribute list.
	 */
	static final ArrayList convertRLAttributeObjectsToRLSAttributes(List results) {
		if (results == null)
			return null;

		ArrayList attrlist = new ArrayList(results.size());
		Iterator iter = results.iterator();
		while (iter.hasNext()) {
			RLSAttributeObject aobj = (RLSAttributeObject) iter.next();
			attrlist.add(aobj.attr);
			aobj.attr = null;
			aobj.key = null;
		}
		results.clear();
		return attrlist;
	}

	/**
	 * Converts List of MappingResult elements to ArrayList of RLSString2Bulk
	 * elements.
	 * 
	 * @param results
	 *            MappingResult list.
	 * @return RLSString2Bulk list.
	 */
	static final ArrayList convertRLIUpdateToRLSString2(List results) {
		if (results == null)
			return null;

		ArrayList s2list = new ArrayList(results.size());
		Iterator iter = results.iterator();
		while (iter.hasNext()) {
			RLIUpdate upd = (RLIUpdate) iter.next();
			String pattern = upd.getPattern();
			if (pattern == null)
				pattern = ""; // Convert null to empty string
			s2list.add(new RLSString2(upd.getUrl(), pattern)); 
		}
		return s2list;
	}
}