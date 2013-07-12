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
package org.globus.replica.rls.impl.rpc;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.globus.gsi.GSIConstants;
import org.globus.gsi.gssapi.GSSConstants;
import org.globus.gsi.gssapi.GlobusGSSManagerImpl;
import org.globus.gsi.gssapi.auth.Authorization;
import org.globus.gsi.gssapi.auth.HostAuthorization;
import org.globus.gsi.gssapi.net.GssSocket;
import org.globus.gsi.gssapi.net.GssSocketFactory;
import org.globus.replica.rls.Administrative;
import org.globus.replica.rls.AttributeResult;
import org.globus.replica.rls.AttributeSearch;
import org.globus.replica.rls.BatchCatalogQuery;
import org.globus.replica.rls.BatchIndexQuery;
import org.globus.replica.rls.CatalogExistenceQuery;
import org.globus.replica.rls.CatalogQuery;
import org.globus.replica.rls.ConfigurationOption;
import org.globus.replica.rls.IndexExistenceQuery;
import org.globus.replica.rls.IndexMapping;
import org.globus.replica.rls.IndexMappingResult;
import org.globus.replica.rls.IndexQuery;
import org.globus.replica.rls.LocalReplicaCatalog;
import org.globus.replica.rls.Mapping;
import org.globus.replica.rls.MappingResult;
import org.globus.replica.rls.QueryResults;
import org.globus.replica.rls.RLIUpdate;
import org.globus.replica.rls.RLSAttribute;
import org.globus.replica.rls.RLSAttributeObject;
import org.globus.replica.rls.RLSClient;
import org.globus.replica.rls.RLSConnection;
import org.globus.replica.rls.RLSException;
import org.globus.replica.rls.RLSIOException;
import org.globus.replica.rls.RLSLRCInfo;
import org.globus.replica.rls.RLSOffsetLimit;
import org.globus.replica.rls.RLSRLIInfo;
import org.globus.replica.rls.RLSStatusCode;
import org.globus.replica.rls.RLSStats;
import org.globus.replica.rls.Rename;
import org.globus.replica.rls.RenameResult;
import org.globus.replica.rls.ReplicaLocationIndex;
import org.globus.replica.rls.Results;
import org.globus.replica.rls.SimpleCatalogQuery;
import org.globus.replica.rls.SimpleIndexQuery;
import org.globus.util.GlobusURL;
import org.gridforum.jgss.ExtendedGSSContext;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;

/**
 * These are implementation details. Too mysterious and spookey for YOU to be
 * looking at, don't you think?
 */
public class SimpleRLSConnection implements RLSConnection {

	private static final int DEFAULT_PORT = 39281;
	private static final String PROTOCOL_SECURE = "rls";
	private static final String PROTOCOL_UNSECURE = "rlsn";

	private static final String CLOSE = "close";

	// CONSTANTS //
	
	private static final String LOCALHOST	= "localhost";
	
	private static final int RPC_NORESPONSE = 1; 
	private static final int RPC_NORESULT = 2; 
	private static final int RPC_SINGLERESULT = 4; 
	private static final int RPC_MULTIPLERESULTS = 8; 
	
	
	// STATIC //
	
    private static Log logger = LogFactory.getLog(SimpleRLSConnection.class.getName());

    
	// FIELDS //

	LocalReplicaCatalog catalog = null;
	ReplicaLocationIndex index = null;
	Administrative admin = null;

	Socket sock = null;
	RPCOutputStream out = null;
	RPCInputStream in = null;

	
	// CONNECTION INTERFACE //

	/**
	 * Implements the RLSConnection interface for clients of the RLS server
	 * using the RPC-style protocol.
	 */
	public SimpleRLSConnection(GlobusURL url, GSSCredential cred)
			throws RLSException {

		// Validate params
		if (url == null)
			throw new IllegalArgumentException("url == null");
		
		// Establish connection according to chosen protocol
		if (PROTOCOL_UNSECURE.equals(url.getProtocol())) {
			_establishUnsecuredConnection(url);
		}
		else if (PROTOCOL_SECURE.equals(url.getProtocol())) {
			_establishSecuredConnection(url, cred);
		}
		else {
			throw new IllegalArgumentException("Protocol not supported (" +
					"protocol: " + url.getProtocol() + ")");
		}
	}

	/**
	 * Establishes a connection to RLS server without security.
	 */
	private void _establishUnsecuredConnection(
			GlobusURL url) throws RLSException {

		// Establish RLS connection
		try {
			// Parse port
			int port = url.getPort();
			if (port < 0)
				port = DEFAULT_PORT;
			
			// Parse host
			String host = url.getHost();
			if (LOCALHOST.equalsIgnoreCase(host))
				host = InetAddress.getLocalHost().getCanonicalHostName();
			
			// Connect to server
			sock = new Socket(host, port);
			
			// Get socket input/output streams
			OutputStream rawOut = sock.getOutputStream();
			InputStream rawIn = sock.getInputStream();
			rawOut.flush(); // Not required to flush

			// Wrap I/O streams with RPC streams
			out = new RPCOutputStream(rawOut);
			in = new RPCInputStream(rawIn);

			// Verify RLS server response
			int rc = in.readInt();
			if (rc != RLSStatusCode.RLS_SUCCESS) {
				logger.debug("Failed to establish RLS connection, RLS server" +
						" returned rc=" + rc);
				_release();
				throw new RLSException(rc, RLSStatusCode.toMessage(rc));
			}

			// Instantiate RLS interfaces
			catalog = new LRCImpl();
			index = new RLIImpl();
			admin = new AdminImpl();
		}
		catch (UnknownHostException e) {
			_release();
			throw new RLSException(RLSStatusCode.RLS_GLOBUSERR, "Unable to " +
					"resolve localhost (cause: " + e.getLocalizedMessage() 
					+ ")", e);
		}
		catch (IOException e) {
			_release();
			throw new RLSException(RLSStatusCode.RLS_GLOBUSERR, "I/O failure" +
					" when establishing connection (cause: " + 
					e.getLocalizedMessage()	+ ")", e);
		}
	}

	/**
	 * Establishes a connection to RLS server with security.
	 */
	private void _establishSecuredConnection(
			GlobusURL url, GSSCredential cred) throws RLSException {

		// Establish RLS connection
		try {
			// Parse port
			int port = url.getPort();
			if (port < 0)
				port = DEFAULT_PORT;
			
			// Parse host
			String host = url.getHost();
			if (LOCALHOST.equalsIgnoreCase(host))
				host = InetAddress.getLocalHost().getCanonicalHostName();
			
			// Instantiate authorization
			Authorization auth = HostAuthorization.getInstance();

			// Instantiate gss manager
			GSSManager manager = GlobusGSSManagerImpl.getInstance();

			// Establish context
			ExtendedGSSContext context = 
				(ExtendedGSSContext) manager.createContext(
						null, GSSConstants.MECH_OID, cred, GSSContext.DEFAULT_LIFETIME);
			context.requestCredDeleg(false);
			context.requestConf(true);
			context.requestAnonymity(false);
			context.setOption(GSSConstants.GSS_MODE, GSIConstants.MODE_GSI);
			
			// Connect to server
			sock = GssSocketFactory.getDefault().createSocket(host, port, context);
			GssSocket gssSock = (GssSocket) sock;
			gssSock.setWrapMode(GssSocket.SSL_MODE);
			gssSock.setAuthorization(auth);

			// Get secure input/output streams
			OutputStream secureOut = sock.getOutputStream();
			InputStream secureIn = sock.getInputStream();
			secureOut.flush(); // Not required to flush
			
			// Get unsecure "raw" input/output streams
			Socket rawSock = gssSock.getWrappedSocket();
			OutputStream rawOut = rawSock.getOutputStream();
			InputStream rawIn = rawSock.getInputStream();
			
			// Due to lack of security on the RLS stream the "raw" I/O streams
			// are used to created the RPC stream. When the RLS server is fixed
			// we will instead use the secure streams below.
			out = new RPCOutputStream(rawOut);
			in = new RPCInputStream(rawIn);
			
			// Ensure context is established
			if (!context.isEstablished()) {
				throw new RLSException(RLSStatusCode.RLS_GLOBUSERR,
						"Security context could not be established.");
			}
			logger.debug("Initiator : " + context.getSrcName());
			logger.debug("Acceptor  : " + context.getTargName());
			logger.debug("Lifetime  : " + context.getLifetime());
			logger.debug("Privacy   : " + context.getConfState());
			logger.debug("Anonymity : " + context.getAnonymityState());

			// Verify RLS server response
			int rc = in.readInt();
			if (rc != RLSStatusCode.RLS_SUCCESS) {
				logger.debug("Failed to establish RLS connection, RLS server" +
						" returned rc=" + rc);
				_release();
				throw new RLSException(rc, RLSStatusCode.toMessage(rc));
			}

			// Instantiate RLS interfaces
			catalog = new LRCImpl();
			index = new RLIImpl();
			admin = new AdminImpl();
			
			// Release context
			context.dispose();
		}
		catch (GSSException e) {
			_release();
			throw new RLSException(RLSStatusCode.RLS_GLOBUSERR, "Failed to " +
					"establish security context (cause: " + 
					e.getLocalizedMessage()	+ ")", e);
		}
		catch (UnknownHostException e) {
			_release();
			throw new RLSException(RLSStatusCode.RLS_GLOBUSERR, "Unable to " +
					"resolve localhost (cause: " + e.getLocalizedMessage() 
					+ ")", e);
		}
		catch (IOException e) {
			_release();
			throw new RLSException(RLSStatusCode.RLS_GLOBUSERR, "I/O failure" +
					" when establishing connection (cause: " + 
					e.getLocalizedMessage()	+ ")", e);
		}
	}
	
	/**
	 * Frees system resources.
	 * @throws IOException
	 */
	private void _release() {
		if (in != null) {
			try { in.close(); } catch (Exception e) {}
			in = null;
		}
		if (out != null) {
			try { out.close(); } catch (Exception e) {}
			out = null;
		}
		if (sock != null) {
			try { sock.close(); } catch (Exception e) {}
			sock = null;
		}
		catalog = null;
		index = null;
		admin = null;
	}

	/**
	 * Set SoTimeout for underlying Socket connection, specified in 
	 * <b>milliseconds</b>.
	 * @param timeout SoTimeout in <b>milliseconds</b>.
	 * @throws RLSException Wrapped SocketException. See {@link java.net.SocketException}.
	 */
	public void setTimeout(int timeout) throws RLSException {
		try {
			sock.setSoTimeout(timeout);
		}
		catch (SocketException e) {
			throw new RLSIOException(e);
		}
	}
	
	/**
	 * Get SoTimeout for underlying Socket connection, value returned in 
	 * <b>milliseconds</b>.
	 * @return SoTimeout in <b>milliseconds</b>.
	 * @throws RLSException Wrapped SocketException. See {@link java.net.SocketException}.
	 */
	public int getTimeout() throws RLSException {
		try {
			return sock.getSoTimeout();
		}
		catch (SocketException e) {
			throw new RLSIOException(e);
		}
	}
	
	public LocalReplicaCatalog catalog() {
		return this.catalog;
	}

	public ReplicaLocationIndex index() {
		return this.index;
	}

	public Administrative admin() {
		return this.admin;
	}

	public void close() throws org.globus.replica.rls.RLSException {
		_call(CLOSE, null, null, null, null, null, RPC_NORESPONSE);
		_release();
	}
	
	// ADMIN INTERFACE //
	class AdminImpl implements Administrative {

		// CONSTANTS //
		
		private static final String SET_CONFIGURATION = "set_configuration";
		private static final String GET_CONFIGURATION = "get_configuration";
		private static final String STATS = "stats";
		private static final String RLI_SENDER_LIST = "rli_sender_list";
		private static final String RLI_RLI_LIST = "rli_rli_list";
		private static final String LRC_RLI_LIST = "lrc_rli_list";
		private static final String LRC_RLI_INFO = "lrc_rli_info";
		private static final String LRC_RLI_GET_PART = "lrc_rli_get_part";
		private static final String LRC_RLI_DELETE = "lrc_rli_delete";
		private static final String LRC_RLI_ADD = "lrc_rli_add";
		private static final String ADMIN = "admin";
		private static final int ADMIN_PING = RLSClient.ADMIN_PING;
		private static final int ADMIN_QUIT = RLSClient.ADMIN_QUIT;
		private static final int ADMIN_UPDATE = RLSClient.ADMIN_SSU;
		
		public void addUpdate(RLIUpdate update) throws RLSException {
			// Validate rliurl
			String rliurl = update.getUrl();
			if (rliurl == null || rliurl.length() == 0) {
				throw new IllegalArgumentException(
						"RLI url is null or empty (rliurl: "+rliurl+")");
			}
			// Validate flags
			int flags = update.getFlags();
			if (flags != 0 && flags != RLSClient.RLIFLAG_BLOOMFILTER) {
				throw new IllegalArgumentException(
						"Invalid argument (flags: "+flags+")");
			}
			
			// Contruct input arguments
			List input = new ArrayList(3);
			input.add(rliurl);
			input.add(Integer.toString(flags));
			input.add(update.getPattern());
			
			// Call server
			_call(LRC_RLI_ADD, null, input, null, 
					stringSerializer, null, RPC_NORESULT);
		}

		public void deleteUpdate(RLIUpdate update) throws RLSException {
			// Validate rliurl
			String rliurl = update.getUrl();
			if (rliurl == null || rliurl.length() == 0) {
				throw new IllegalArgumentException(
						"RLI url is null or empty (rliurl: "+rliurl+")");
			}
			
			// Contruct input arguments
			List input = new ArrayList(1);
			input.add(update);
			
			// Call server
			_call(LRC_RLI_DELETE, null, input, null, 
					rliUpdateSerializer, null, RPC_NORESULT);
		}

		public RLSRLIInfo findUpdate(String rliurl) throws RLSException {
			// Construct input args
			List input = new ArrayList(1);
			input.add(rliurl);
			
			// Call server
			Results results = _call(LRC_RLI_INFO, null, input, null, 
					stringSerializer, rliInfoDeserializer, RPC_SINGLERESULT);
			return (RLSRLIInfo) results.getBatch().get(0);
		}

		public List getConfiguration(String option) throws RLSException {
			// Construct input args
			if (option == null)
				option = "";
			List input = new ArrayList(1);
			input.add(option);
			
			// Call server
			Results results = _call(GET_CONFIGURATION, null, input, null, 
					stringSerializer, configOptionDeserializer, RPC_MULTIPLERESULTS);
			return results.getBatch();
		}

		public List listRliToRliUpdates() throws RLSException {
			// Call server
			Results results = _call(RLI_RLI_LIST, null, null, null,
					null, rliInfoDeserializer, RPC_MULTIPLERESULTS);
			return results.getBatch();
		}

		public List listUpdatePartitions(RLIUpdate update) throws RLSException {
			// Contruct input arguments
			List input = new ArrayList(1);
			input.add(update);
			
			// Call server
			Results results = _call(LRC_RLI_GET_PART, null, input, null, 
					rliUpdateSerializer, rliUpdateDeserializer, RPC_MULTIPLERESULTS);
			return results.getBatch();
		}

		public List listUpdates() throws RLSException {
			// Call server
			Results results = _call(LRC_RLI_LIST, null, null, null,
					null, rliInfoDeserializer, RPC_MULTIPLERESULTS);
			return results.getBatch();
		}

		public void ping() throws RLSException {
			// Construct input
			List input = new ArrayList(1);
			input.add(Integer.toString(ADMIN_PING));
			
			// Call server
			_call(ADMIN, null, input, null,
					stringSerializer, null, RPC_NORESULT);
		}

		public void quit() throws RLSException {
			// Construct input
			List input = new ArrayList(1);
			input.add(Integer.toString(ADMIN_QUIT));
			
			// Call server
			_call(ADMIN, null, input, null,
					stringSerializer, null, RPC_NORESULT);
		}

		public void setConfiguration(String name, String value) throws RLSException {
			// Construct input args
			List input = new ArrayList(2);
			input.add(name);
			input.add(value);
			
			// Call server
			_call(SET_CONFIGURATION, null, input, null, 
					stringSerializer, null, RPC_NORESULT);
		}

		public RLSStats stats() throws RLSException {
			// Call server
			Results results = _call(STATS, null, null, null,
					null, statsDeserializer, RPC_SINGLERESULT);
			return (RLSStats) results.getBatch().get(0);
		}

		public void update() throws RLSException {
			// Construct input
			List input = new ArrayList(1);
			input.add(Integer.toString(ADMIN_UPDATE));
			
			// Call server
			_call(ADMIN, null, input, null,
					stringSerializer, null, RPC_NORESULT);
		}

		public List updatedBy() throws RLSException {
			// Call server
			Results results = _call(RLI_SENDER_LIST, null, null, null,
					null, senderInfoDeserializer, RPC_MULTIPLERESULTS);
			return results.getBatch();
		}
	} // END AdminImpl //
	
	
	// LRC INTERFACE //
	class LRCImpl implements LocalReplicaCatalog {

		// CONSTANTS //
		
		private static final String LRC_ATTR_GET = "lrc_attr_get";
		private static final String LRC_RENAMEPFN_BULK = "lrc_renamepfn_bulk";
		private static final String LRC_RENAMELFN_BULK = "lrc_renamelfn_bulk";
		private static final String LRC_ADD_BULK = "lrc_add_bulk";
		private static final String LRC_CREATE_BULK = "lrc_create_bulk";
		private static final String LRC_DELETE_BULK = "lrc_delete_bulk";
		private static final String LRC_EXISTS_BULK = "lrc_exists_bulk";
		private static final String LRC_ATTR_SEARCH = "lrc_attr_search";
		private static final String LRC_GET_PFN_BULK = "lrc_get_pfn_bulk";
		private static final String LRC_GET_LFN_BULK = "lrc_get_lfn_bulk";
		private static final String LRC_ATTR_VALUE_GET_BULK = "lrc_attr_value_get_bulk";
		private static final String LRC_ATTR_REMOVE_BULK = "lrc_attr_remove_bulk";
		private static final String LRC_ATTR_ADD_BULK = "lrc_attr_add_bulk";
		private static final String LRC_ATTR_MODIFY = "lrc_attr_modify";
		private static final String LRC_ATTR_DELETE = "lrc_attr_delete";
		private static final String LRC_ATTR_CREATE = "lrc_attr_create";
		private static final String LRC_MAPPING_EXISTS = "lrc_mapping_exists";
		private static final String LRC_GET_PFN_WC = "lrc_get_pfn_wc";
		private static final String LRC_GET_LFN_WC = "lrc_get_lfn_wc";
		private static final String LRC_GET_PFN = "lrc_get_pfn";
		private static final String LRC_GET_LFN = "lrc_get_lfn";

		public List createMappings(List mappings) throws RLSException {
			Results r = _callBulk(LRC_CREATE_BULK, null, mappings,
					mappingSerializer, mappingUpdateDeserializer);
			return r.getBatch();
		}

		public List deleteMappings(List mappings) throws RLSException {
			Results r = _callBulk(LRC_DELETE_BULK, null, mappings,
					mappingSerializer, mappingUpdateDeserializer);
			return r.getBatch();
		}

		public List addMappings(List mappings) throws RLSException {
			Results r = _callBulk(LRC_ADD_BULK, null, mappings,
					mappingSerializer, mappingUpdateDeserializer);
			return r.getBatch();
		}

		public List defineAttributes(List attributes) throws RLSException {
			// Iterate over the input parameters and make individual operation
			// calls, then collect results in order to 'bulkify' the call
			List results = new LinkedList();
			List input = new ArrayList(1);
			Iterator itr = attributes.iterator(); 
			while(itr.hasNext()) {
				RLSAttribute attr = (RLSAttribute) itr.next();
				input.clear();
				input.add(attr);
				int rc;
				try {
					Results subr = _call(LRC_ATTR_CREATE, null, input, null,
							attrdefSerializer, null, RPC_NORESULT);
					rc = subr.getRC();
				}
				catch (RLSException ex) {
					rc = ex.GetRC();
				}
				// Only report failures
				if (rc != RLSStatusCode.RLS_SUCCESS)
					results.add(new AttributeResult(rc, null, attr.name));
			}
			return results;
		}

		public List undefineAttributes(List attributes, boolean clearvalues)
				throws RLSException {
			// Iterate over the input parameters and make individual operation
			// calls, then collect results in order to 'bulkify' the call
			List results = new LinkedList();
			List suffix = new ArrayList(1);
			suffix.add((clearvalues) ? new Integer(1) : new Integer(0));
			List input = new ArrayList(1);
			Iterator itr = attributes.iterator(); 
			while(itr.hasNext()) {
				RLSAttribute attr = (RLSAttribute) itr.next();
				input.clear();
				input.add(attr);
				int rc;
				try {
					Results subr = _call(LRC_ATTR_DELETE, null, input, suffix,
							minAttrSerializer, null, RPC_NORESULT);
					rc = subr.getRC();
				}
				catch (RLSException ex) {
					rc = ex.GetRC();
				}
				// Only report failures
				if (rc != RLSStatusCode.RLS_SUCCESS)
					results.add(new AttributeResult(rc, null, attr.name));
			}
			return results;
		}

		public List addAttributes(List attributes) throws RLSException {
			Results r = _callBulk(LRC_ATTR_ADD_BULK, null, attributes,
					attrobjSerializer, attrObjResultDeserializer);
			return r.getBatch();
		}

		public List modifyAttributes(List attributes) throws RLSException {
			// Iterate over the input parameters and make individual operation
			// calls, then collect results in order to 'bulkify' the call
			List results = new LinkedList();
			List input = new ArrayList(1);
			Iterator itr = attributes.iterator(); 
			while(itr.hasNext()) {
				RLSAttributeObject ao = (RLSAttributeObject) itr.next();
				input.clear();
				input.add(ao);
				int rc;
				try {
					Results subr = _call(LRC_ATTR_MODIFY, null, input, null,
							modifyAttrobjSerializer, null, RPC_NORESULT);
					rc = subr.getRC();
				}
				catch (RLSException ex) {
					rc = ex.GetRC();
				}
				// Only report failures
				if (rc != RLSStatusCode.RLS_SUCCESS)
					results.add(new AttributeResult(rc, null, ao.attr.name));
			}
			return results;
		}

		public List removeAttributes(List attributes) throws RLSException {
			Results r = _callBulk(LRC_ATTR_REMOVE_BULK, null, attributes,
					removeAttrobjSerializer, attrObjResultDeserializer);
			return r.getBatch();
		}

		public List renameLogicalNames(List renames) throws RLSException {
			Results r = _callBulk(LRC_RENAMELFN_BULK, null, renames,
					renameSerializer, renameResultDeserializer);
			return r.getBatch();
		}

		public List renameTargetNames(List renames) throws RLSException {
			Results r = _callBulk(LRC_RENAMEPFN_BULK, null, renames,
					renameSerializer, renameResultDeserializer);
			return r.getBatch();
		}
		
		// LRC QUERY METHODS //
		
		public Results query(CatalogQuery query) throws RLSException {

			if (query instanceof CatalogExistenceQuery) {
				return _existenceQuery((CatalogExistenceQuery) query);
			} else if (query instanceof SimpleCatalogQuery) {
				return _simpleQuery((SimpleCatalogQuery) query);
			} else if (query instanceof AttributeSearch) {
				return _attributeSearch((AttributeSearch) query);
			} else if (query instanceof BatchCatalogQuery) {
				return _batchQuery((BatchCatalogQuery) query);
			} else {
				throw new IllegalArgumentException("Unsupported query object" +
						" (object: " + query.toString() + ")");
			}
		}

		/** Helper method for existence queries. */
		private Results _existenceQuery(CatalogExistenceQuery query)
			throws RLSException {
			
			// Determine existence query type: object or mapping
			if (CatalogExistenceQuery.objectExists.equals(
					query.getType())) {
				
				List prefix = new ArrayList(1);
				prefix.add(query.getObjectType());
				return _query(LRC_EXISTS_BULK, prefix, query.getBatch(), 
						stringSerializer, mappingExistsDeserializer);
			}
			else if (CatalogExistenceQuery.mappingExists.equals(
					query.getType())) {
				
				// RLS does not support bulk mapping exists, so we must make
				// multiple calls instead
				QueryResults results = new QueryResults(
						RLSStatusCode.RLS_SUCCESS,
						new LinkedList());
				List input = new ArrayList(1);
				Iterator itr = query.getBatch().iterator(); 
				while(itr.hasNext()) {
					Mapping map = (Mapping) itr.next();
					input.clear();
					input.add(map);
					int rc;
					try {
						Results subr = _call(LRC_MAPPING_EXISTS, null, 
								input, null, mappingSerializer, null, 
								RPC_NORESULT);
						rc = subr.getRC();
					}
					catch (RLSException ex) {
						rc = ex.GetRC();
					}
					results.getBatch().add(new MappingResult(rc, map));
				}
				return results;
			}
			else {
				throw new IllegalArgumentException("Unsupport existence query "+
						"type (type: " + query.getObjectType() + ")");
			}
		}

		/** Helper method for simple queries. */
		private Results _simpleQuery(SimpleCatalogQuery query)
			throws RLSException {
			
			// Determine method
			String method;
			Integer type = query.getType();
			if (SimpleCatalogQuery.queryMappingsByLogicalNamePattern.equals(type)) {
				method = LRC_GET_PFN_WC;
			} else if (SimpleCatalogQuery.queryMappingsByTargetNamePattern.equals(type)) {
				method = LRC_GET_LFN_WC;
			} else if (SimpleCatalogQuery.queryMappingsByLogicalName.equals(type)) {
				method = LRC_GET_PFN;
			} else if (SimpleCatalogQuery.queryMappingsByTargetName.equals(type)) {
				method = LRC_GET_LFN;
			} else if (SimpleCatalogQuery.queryAttributeDefinitions.equals(type)) {
				method = LRC_ATTR_GET;
			} else {
				throw new IllegalArgumentException("Unsupported query type" +
						" (type: " + type + ")");
			}
			
			// Determine serializer/deserializer
			RPCObjectSerializer ser;
			RPCObjectDeserializer deser;
			if (query.getParam() instanceof RLSAttribute) {
				ser = minAttrSerializer;
				deser = new AttrDeserializer(
						((RLSAttribute)query.getParam()).GetObjType());
			}
			else {
				ser = stringSerializer;
				deser = simpleMappingResultDeserializer;
			}
			
			// Create input params
			List input = new ArrayList(1);
			input.add(query.getParam());
			
			// Call server
			return _query(method, null, input, 
					query.getOffsetLimit(), ser, deser);
		}
		
		/** Helper method for attribute search operation */
		private Results _attributeSearch(AttributeSearch query)
			throws RLSException {

			// Construct input args
			List input = new ArrayList(1);
			input.add(query);
			
			// Initialize deserializer
			AttrSearchResultDeserializer deser =
				new AttrSearchResultDeserializer(
						query.getName(), query.getObjtype());
			
			return _query(LRC_ATTR_SEARCH, null, input,
					query.getOffsetLimit(),
					attributeSearchSerializer, deser);
		}

		/** Helper method for batch queries. */
		private Results _batchQuery(BatchCatalogQuery query)
			throws RLSException {

			// Check query type
			Integer queryType = query.getType();
			if (BatchCatalogQuery.attributeQuery.equals(queryType)) {

				// Create prefix from attr name and obj type
				List prefix = new ArrayList(2);
				prefix.add(query.getAttributeName());
				prefix.add(query.getAttributeObjectType().toString());

				// Keys
				List keys = query.getBatch();

				// Initialize deserializer
				AttrValGetResultDeserializer deser = 
					new AttrValGetResultDeserializer(
							query.getAttributeObjectType().intValue());

				// Attribute (value) query
				return _callBulk(LRC_ATTR_VALUE_GET_BULK, prefix, keys, 
						stringSerializer, deser);

			} else if (BatchCatalogQuery.mappingQueryByLogicalNames.equals(
					queryType)) {
				
				// Logical name query
				return _query(LRC_GET_PFN_BULK, null, 
						query.getBatch(), 
						stringSerializer, mappingResultDeserializer);

			} else if (BatchCatalogQuery.mappingQueryByTargetNames.equals(
					queryType)) {

				// Target name query
				return _query(LRC_GET_LFN_BULK, null,
						query.getBatch(), 
						stringSerializer, mappingResultDeserializer);
			} else {
				throw new IllegalArgumentException("Unsupported query type" +
						" (type: " + queryType + ")");
			}
		}
	} // END LRCImpl
	
	
	// RLI INTERFACE //
	class RLIImpl implements ReplicaLocationIndex {

		private static final String RLI_MAPPING_EXISTS = "rli_mapping_exists";
		private static final String RLI_EXISTS_BULK = "rli_exists_bulk";
		private static final String RLI_GET_LRC_BULK = "rli_get_lrc_bulk";
		private static final String RLI_GET_LRC_WC = "rli_get_lrc_wc";
		private static final String RLI_GET_LRC = "rli_get_lrc";

		public Results query(IndexQuery query) throws RLSException {
			if (query instanceof SimpleIndexQuery)
				return _simpleIndexQuery((SimpleIndexQuery) query);
			else if (query instanceof BatchIndexQuery)
				return _batchIndexQuery((BatchIndexQuery) query);
			else if (query instanceof IndexExistenceQuery)
				return _existenceQuery((IndexExistenceQuery) query);
			else
				throw new IllegalArgumentException("Unsupported query object");
		}
		
		private Results _simpleIndexQuery(SimpleIndexQuery query) throws RLSException {
			// Determine method
			String method;
			if (SimpleIndexQuery.queryMappingsByLogicalName.equals(
					query.getType())) {
				method = RLI_GET_LRC;
			}
			else if (SimpleIndexQuery.queryMappingsByLogicalNamePattern.equals(
					query.getType())) {
				method = RLI_GET_LRC_WC;
			}
			else {
				throw new IllegalArgumentException("Invalid query type");
			}
			
			// Construct params
			List input = new ArrayList(1);
			input.add(query.getParam());
			
			// Call server
			return _query(method, null, input, 
					query.getOffsetLimit(), 
					stringSerializer, simpleIndexMappingResultDeserializer);
		}
		
		private Results _batchIndexQuery(BatchIndexQuery query) throws RLSException {
			// Determine method
			String method;
			if (BatchIndexQuery.queryMappingsByLogicalNames.equals(
					query.getType())) {
				method = RLI_GET_LRC_BULK;
			}
			else {
				throw new IllegalArgumentException("Invalid query type");
			}
			
			// Call server
			return _query(method, null, query.getBatch(), 
					stringSerializer, indexMappingResultDeserializer);
		}
		
		private Results _existenceQuery(IndexExistenceQuery query) throws RLSException {

			if (IndexExistenceQuery.objectExists.equals(
					query.getType())) {

				// Make prefix
				List prefix = new ArrayList(1);
				prefix.add(query.getObjectType().toString());
				
				return _query(RLI_EXISTS_BULK, prefix, query.getBatch(), 
						stringSerializer, indexMappingExistsDeserializer);

			} else if (IndexExistenceQuery.mappingExists.equals(
					query.getType())) {

				throw new RLSException(RLSStatusCode.RLS_UNSUPPORTED,
						"This method is unsupported. If this method is " +
						"critical to your usage of RLS, please notify the " +
						"RLS developers at rls-dev@globus.org.");
				/*
				 * NOTE: Using the "rli_mapping_exists" RPC method on a call to
				 * the RLS server causes an I/O mismatch that results in the
				 * client not returning from the query. The method itself works
				 * with the RLS C client. In order to fix this method, we need
				 * to evaluate the RPC protocol more closely. For now, we have
				 * avoided this task. If you are a user that needs this method,
				 * please inform the RLS developers.  
				 *
				// RLS does not support bulk mapping exists, so we must make
				// multiple calls instead
				QueryResults results = new QueryResults(
						RLSResultCode.RLS_SUCCESS,
						new LinkedList());
				List prefix = null;
				List input = new ArrayList(1);
				
				Iterator itr = query.getBatch().iterator(); 
				while(itr.hasNext()) {
					IndexMapping map = (IndexMapping) itr.next();
					input.clear();
					input.add(map);
					int rc;
					try {
						Results subr = _call(RLI_MAPPING_EXISTS, prefix, input, null,
									indexMappingSerializer, null, RPC_NORESULT);
						rc = subr.getRC();
					}
					catch (RLSException ex) {
						rc = ex.GetRC();
					}
					results.getBatch().add(new IndexMappingResult(rc, map));
				}
				return results;
				 *
				 */				
			} else {
				throw new IllegalArgumentException("Unsupported query object");
			}
		}
	} // RLIImpl

	
	// RPC PROXY METHODS //
	
	/**
	 * Query interface that supports bulk operations.
	 * @param method
	 * @param prefix
	 * @param input
	 * @param serializer
	 * @param deserializer
	 * @return
	 * @throws RLSException
	 */
	private Results _query(String method, List prefix, List input, 
			RPCObjectSerializer serializer,
			RPCObjectDeserializer deserializer) throws RLSException { 
		if (input == null)
			throw new NullPointerException("Input list is null");
		if (input.isEmpty())
			throw new IllegalArgumentException("Input list is empty");
		if (input.get(0) == null) {
			throw new IllegalArgumentException("Input list contains" +
					" null object");
		}
		if (!serializer.isValidInstance(input.get(0))) {
			throw new IllegalArgumentException("Input list contains" +
					" object of type " + 
					input.get(0).getClass().getName()
					+ " which is not supported by this operation");
		}

		int rc;
		List results;
		try {
			// Check input stream
			_checkInputStream(in);
			
			out.writeString(method);
			
			if (prefix != null) {
				for (Iterator i = prefix.iterator(); i.hasNext(); ) {
					out.writeString(i.next().toString());
				}
			}
			
			for (Iterator i = input.iterator(); i.hasNext(); ) {
				serializer.serialize(out, i.next());
			}
			out.writeTerminator();
			
			// Read global result code
			rc = in.readInt();
			if (rc != RLSStatusCode.RLS_SUCCESS) {
				throw new RLSException(rc, in.readString());
			}

			// Read results
			results = new LinkedList();
			for (Object obj = deserializer.deserialize(in); 
					obj != null; obj = deserializer.deserialize(in)) {
				results.add(obj);
			}

		} catch (IOException e) {
			throw new RLSIOException(RLSStatusCode.RLS_GLOBUSERR, 
					"I/O failure (cause: " + e.getLocalizedMessage() + ")", e);
		} catch (RPCDeserializationException e) {
			throw new RLSException(RLSStatusCode.RLS_GLOBUSERR,
					"Deserialization failure (cause: " + 
					e.getLocalizedMessage() + ")", e);
		}
		
		return new QueryResults(rc, results);
	}

	/**
	 * This query interface supports non-bulk methods.
	 * @param method
	 * @param prefix
	 * @param input
	 * @param offsetLimit
	 * @param serializer
	 * @param deserializer
	 * @return
	 * @throws RLSException
	 */
	private Results _query(String method, List prefix, List input, 
			RLSOffsetLimit offsetLimit,
			RPCObjectSerializer serializer,
			RPCObjectDeserializer deserializer) throws RLSException { 
		if (input == null)
			throw new NullPointerException("Input list is null");
		if (input.isEmpty())
			throw new IllegalArgumentException("Input list is empty");
		if (input.get(0) == null) {
			throw new IllegalArgumentException("Input list contains" +
					" null object");
		}
		if (!serializer.isValidInstance(input.get(0))) {
			throw new IllegalArgumentException("Input list contains" +
					" object of type " + 
					input.get(0).getClass().getName()
					+ " which is not supported by this operation");
		}

		List results;
		if (offsetLimit == null || offsetLimit.reslimit <= 0) {
			offsetLimit = new RLSOffsetLimit(0,0);
			// Single query when offsetLimit not specified or reslimit == 0
			results = _subquery(method, prefix, input, offsetLimit, 
					serializer, deserializer);
		}
		else {
			// Get first set of subquery results
			results = new LinkedList();
			List subqresults = _subquery(method, prefix, input, offsetLimit, 
					serializer, deserializer);
			
			// Execute subqueries until no more results available
			while (!subqresults.isEmpty()) {
				results.addAll(subqresults);
				subqresults = _subquery(method, prefix, input, offsetLimit, 
						serializer, deserializer);
			}
		}
		
		return new QueryResults(RLSStatusCode.RLS_SUCCESS, results);
	}

	/**
	 * Processes the subqueries of queries with offset and result limit
	 * specified.
	 * 
	 * @param method The method name.
	 * @param prefix The prefix. MAY BE null.
	 * @param input The input list.
	 * @param offsetLimit MUST NOT be null.
	 * @param serializer The input serializer.
	 * @param deserializer The output deserializer.
	 * @return The result list. NEVER null. MAY BE empty.
	 * @throws RLSException The results code returned from the RLS server
	 * 			indicated a failure.
	 */
	private List _subquery(String method, List prefix, List input, 
			RLSOffsetLimit offsetLimit,
			RPCObjectSerializer serializer,
			RPCObjectDeserializer deserializer) throws RLSException { 

		int rc;
		List results;
		try {
			// Check input stream
			_checkInputStream(in);
			
			out.writeString(method);
			
			if (prefix != null) {
				for (Iterator i = prefix.iterator(); i.hasNext(); ) {
					out.writeString(i.next().toString());
				}
			}
			
			for (Iterator i = input.iterator(); i.hasNext(); ) {
				serializer.serialize(out, i.next());
			}

			// offsetLimit expected in subqueries
			out.writeInt(offsetLimit.offset);
			out.writeInt(offsetLimit.reslimit);
			
			// Read global result code
			rc = in.readInt();
			if (rc != RLSStatusCode.RLS_SUCCESS) {
				throw new RLSException(rc, in.readString());
			}

			// Read results
			results = new LinkedList();
			for (Object obj = deserializer.deserialize(in); 
					obj != null; obj = deserializer.deserialize(in)) {
				
				if (obj instanceof ResultsContinuationMarker)
					break;
				
				results.add(obj);
				offsetLimit.offset++;
			}

		} catch (IOException e) {
			throw new RLSIOException(RLSStatusCode.RLS_GLOBUSERR, 
					"I/O failure (cause: " + e.getLocalizedMessage() + ")", e);
		} catch (RPCDeserializationException e) {
			throw new RLSException(RLSStatusCode.RLS_GLOBUSERR,
					"Deserialization failure (cause: " + 
					e.getLocalizedMessage() + ")", e);
		}
		
		return results;
	}

	/**
	 * Remote call for non-bulk operations.
	 * @param method
	 * @param prefix
	 * @param input
	 * @param suffix
	 * @param serializer
	 * @param deserializer
	 * @return
	 * @throws RLSException
	 */
	private Results _call(String method, List prefix, List input, List suffix, 
			RPCObjectSerializer serializer,
			RPCObjectDeserializer deserializer, 
			int options) throws RLSException {

		// Input not accepted when serializer is null
		if (serializer == null && input != null) {
			throw new IllegalArgumentException("Input must be null when " +
					"serializer is null");
		}
		else if (serializer != null) {
			if (input == null)
				throw new NullPointerException("Input list is null");
			if (input.isEmpty())
				throw new IllegalArgumentException("Input list is empty");
			if (input.get(0) == null) {
				throw new IllegalArgumentException("Input list contains" +
						" null object");
			}
			if (!serializer.isValidInstance(input.get(0))) {
				throw new IllegalArgumentException("Input list contains" +
						" object of type " + 
						input.get(0).getClass().getName()
						+ " which is not supported by this operation");
			}
		}

		int rc;
		List results = null;
		try {
			// Check input stream
			_checkInputStream(in);
			
			out.writeString(method);
			
			if (prefix != null) {
				for (Iterator i = prefix.iterator(); i.hasNext(); ) {
					out.writeString(i.next().toString());
				}
			}

			if (input != null) {
				for (Iterator i = input.iterator(); i.hasNext(); ) {
					serializer.serialize(out, i.next());
				}
			}
			
			if (suffix != null) {
				for (Iterator i = suffix.iterator(); i.hasNext(); ) {
					out.writeString(i.next().toString());
				}
			}

			// Check response, if necessary
			rc = RLSStatusCode.RLS_SUCCESS;
			if ((options & RPC_NORESPONSE) == 0) {
				// Read global result code
				rc = in.readInt();
				if (rc != RLSStatusCode.RLS_SUCCESS) {
					throw new RLSException(rc, in.readString());
				}
	
				// Read results, if necessary
				results = new LinkedList();
				if ((options & RPC_SINGLERESULT) > 0) {
					results.add(deserializer.deserialize(in));
				}
				else if ((options & RPC_MULTIPLERESULTS) > 0) {
					for (Object obj = deserializer.deserialize(in); 
							obj != null; obj = deserializer.deserialize(in)) {
						results.add(obj);
					}
				}
			}

		} catch (IOException e) {
			throw new RLSIOException(RLSStatusCode.RLS_GLOBUSERR, 
					"I/O failure (cause: " + e.getLocalizedMessage() + ")", e);
		} catch (RPCDeserializationException e) {
			throw new RLSException(RLSStatusCode.RLS_GLOBUSERR,
					"Deserialization failure (cause: " + 
					e.getLocalizedMessage() + ")", e);
		}
		
		return new QueryResults(rc, results);
	}
	
	/**
	 * Remote call for bulk operations.
	 * @param method
	 * @param prefix
	 * @param input
	 * @param serializer
	 * @param deserializer
	 * @return
	 * @throws RLSException
	 */
	private Results _callBulk(String method, List prefix, List input, 
			RPCObjectSerializer serializer,
			RPCObjectDeserializer deserializer) throws RLSException { 
		if (input == null)
			throw new NullPointerException("Input list is null");
		if (input.isEmpty())
			throw new IllegalArgumentException("Input list is empty");
		if (input.get(0) == null) {
			throw new IllegalArgumentException("Input list contains" +
					" null object");
		}
		if (!serializer.isValidInstance(input.get(0))) {
			throw new IllegalArgumentException("Input list contains" +
					" object of type " + 
					input.get(0).getClass().getName()
					+ " which is not supported by this operation");
		}

		int rc;
		List results;
		try {
			// Check input stream
			_checkInputStream(in);
			
			out.writeString(method);
			
			if (prefix != null) {
				for (Iterator i = prefix.iterator(); i.hasNext(); ) {
					out.writeString(i.next().toString());
				}
			}
			
			for (Iterator i = input.iterator(); i.hasNext(); ) {
				serializer.serialize(out, i.next());
			}
			out.writeTerminator();
			
			// Read global result code
			rc = in.readInt();
			if (rc != RLSStatusCode.RLS_SUCCESS) {
				throw new RLSException(rc, in.readString());
			}

			// Read results
			results = new LinkedList();
			for (Object obj = deserializer.deserialize(in); 
					obj != null; obj = deserializer.deserialize(in)) {
				results.add(obj);
			}

		} catch (IOException e) {
			throw new RLSIOException(RLSStatusCode.RLS_GLOBUSERR, 
					"I/O failure (cause: " + e.getLocalizedMessage() + ")", e);
		} catch (RPCDeserializationException e) {
			throw new RLSException(RLSStatusCode.RLS_GLOBUSERR,
					"Deserialization failure (cause: " + 
					e.getLocalizedMessage() + ")", e);
		}
		
		return new QueryResults(rc, results);
	}
	
	/** Check input steam to see if bytes are (unexpectedly) waiting to be read.
	 */
	private void _checkInputStream(InputStream in) {
		try {
			while (in.available() > 0) {
				logger.warn("Unexpected bytes are waiting on input steam. " +
						"Skipping " + in.available() + " bytes.");
				in.skip(in.available());
			}
		}
		catch (Exception e) {
			logger.warn("Exception when checking for available bytes on the " +
					"input stream: " + e.getLocalizedMessage());
		}
	}

	
	// SERIALIZERS //

	/** Strings */
	class StringSerializer implements RPCObjectSerializer {

		public void serialize(RPCOutputStream out, Object obj) throws IOException {
			out.writeString((String)obj);
		}

		public boolean isValidInstance(Object obj) {
			return (obj == null || obj instanceof java.lang.String);
		}
	}
	StringSerializer stringSerializer = new StringSerializer();

	/** Mappings */
	class MappingSerializer implements RPCObjectSerializer {

		public void serialize(RPCOutputStream out, Object obj) throws IOException {
			Mapping m = (Mapping) obj;
			out.writeString(m.getLogical());
			out.writeString(m.getTarget());
		}

		public boolean isValidInstance(Object obj) {
			return (obj instanceof Mapping);
		}
	}
	MappingSerializer mappingSerializer = new MappingSerializer();

	/** Index Mappings */
	class IndexMappingSerializer implements RPCObjectSerializer {

		public void serialize(RPCOutputStream out, Object obj) throws IOException {
			IndexMapping m = (IndexMapping) obj;
			out.writeString(m.getLogical());
			out.writeString(m.getCatalog());
		}

		public boolean isValidInstance(Object obj) {
			return (obj instanceof IndexMapping);
		}
	}
	IndexMappingSerializer indexMappingSerializer = new IndexMappingSerializer();

	/** Minimal attribute serializer */
	class MinimalAttributeSerializer implements RPCObjectSerializer {

		public void serialize(RPCOutputStream out, Object obj) throws IOException {
			RLSAttribute a = (RLSAttribute) obj;
			out.writeString(a.name);
			out.writeInt(a.GetObjType());
		}

		public boolean isValidInstance(Object obj) {
			return (obj instanceof RLSAttribute);
		}
	}
	MinimalAttributeSerializer minAttrSerializer = new MinimalAttributeSerializer ();

	/** Define attribute */
	class AttributeDefineSerializer implements RPCObjectSerializer {

		public void serialize(RPCOutputStream out, Object obj) throws IOException {
			RLSAttribute a = (RLSAttribute) obj;
			out.writeString(a.name);
			out.writeInt(a.GetObjType());
			out.writeInt(a.GetValType());
		}

		public boolean isValidInstance(Object obj) {
			return (obj instanceof RLSAttribute);
		}
	}
	AttributeDefineSerializer attrdefSerializer = new AttributeDefineSerializer();

	/** Writes complete attribute object */
	class AttributeObjectSerializer implements RPCObjectSerializer {

		public void serialize(RPCOutputStream out, Object obj) throws IOException {
			RLSAttributeObject ao = (RLSAttributeObject) obj;
			out.writeString(ao.key);
			out.writeInt(ao.attr.GetObjType());
			out.writeInt(ao.attr.GetValType());
			out.writeString(ao.attr.name);
			serializeAttributeValue(out, ao.attr);
		}
		
		protected void serializeAttributeValue(
				RPCOutputStream out, RLSAttribute attr) throws IOException {
			int valtype = attr.GetValType();
			if (valtype == RLSAttribute.STR)
				out.writeString(attr.GetStrVal());
			else if (valtype == RLSAttribute.INT)
				out.writeInt(attr.GetIntVal());
			else if (valtype == RLSAttribute.DOUBLE)
				out.writeDouble(attr.GetDoubleVal());
			else if (valtype == RLSAttribute.DATE) {
				out.writeDate(attr.GetDateVal());
			}
			else
				throw new IllegalArgumentException("Unknown attribute value type");
		}

		public boolean isValidInstance(Object obj) {
			return (obj instanceof RLSAttributeObject);
		}
	}
	AttributeObjectSerializer attrobjSerializer = new AttributeObjectSerializer();

	/** Removing an attribute involves fewer fields than adding. */
	class RemoveAttributeObjectSerializer extends AttributeObjectSerializer {

		public void serialize(RPCOutputStream out, Object obj) throws IOException {
			RLSAttributeObject ao = (RLSAttributeObject) obj;
			out.writeString(ao.key);
			out.writeInt(ao.attr.GetObjType());
			out.writeString(ao.attr.name);
		}
	}
	RemoveAttributeObjectSerializer removeAttrobjSerializer = new RemoveAttributeObjectSerializer();

	/** Modifying an attribute involves different field ordering than adding/removing */
	class ModifyAttributeObjectSerializer extends AttributeObjectSerializer {

		public void serialize(RPCOutputStream out, Object obj) throws IOException {
			RLSAttributeObject ao = (RLSAttributeObject) obj;
			out.writeString(ao.key);
			out.writeString(ao.attr.name);
			out.writeInt(ao.attr.GetObjType());
			out.writeInt(ao.attr.GetValType());
			serializeAttributeValue(out, ao.attr);
		}
	}
	ModifyAttributeObjectSerializer modifyAttrobjSerializer = new ModifyAttributeObjectSerializer();

	/** AttributeSearch requires its own serializer. */
	class AttributeSearchSerializer extends AttributeObjectSerializer {
		
		public void serialize(RPCOutputStream out, Object obj) throws IOException {
			AttributeSearch as = (AttributeSearch) obj;
			out.writeString(as.getName());
			out.writeInt(as.getObjtype());
			out.writeInt(as.getOp());
			// Operand 1
			if (as.getOp1() != null)
				serializeAttributeValue(out, as.getOp1());
			else
				out.writeString("");
			// Operand 2
			if (as.getOp2() != null)
				serializeAttributeValue(out, as.getOp2());
			else
				out.writeString("");
		}
		
		public boolean isValidInstance(Object obj) {
			return (obj instanceof AttributeSearch);
		}
	}
	AttributeSearchSerializer attributeSearchSerializer =
		new AttributeSearchSerializer();

	/** Renames */
	class RenameSerializer implements RPCObjectSerializer {

		public void serialize(RPCOutputStream out, Object obj) throws IOException {
			Rename r = (Rename) obj;
			out.writeString(r.getFrom());
			out.writeString(r.getTo());
		}

		public boolean isValidInstance(Object obj) {
			return (obj instanceof Rename);
		}
	}
	RenameSerializer renameSerializer = new RenameSerializer();

	/** RLIUpdate */
	class RLIUpdateSerializer implements RPCObjectSerializer {

		public void serialize(RPCOutputStream out, Object obj) throws IOException {
			RLIUpdate r = (RLIUpdate) obj;
			out.writeString(r.getUrl());
			out.writeString(r.getPattern());
		}

		public boolean isValidInstance(Object obj) {
			return (obj instanceof RLIUpdate);
		}
	}
	RLIUpdateSerializer rliUpdateSerializer = new RLIUpdateSerializer();

	
	// DESERIALIZERS //
	
	/** Reads string from non-bulk call (no bulk rc). */
	class StringResultDeserializer implements RPCObjectDeserializer {

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			return in.readString();
		}
	}
	StringResultDeserializer stringDeserializer = 
		new StringResultDeserializer();

	/** Reads bulk query mapping results: rc, logical/target */
	class MappingResultDeserializer implements RPCObjectDeserializer {

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			Integer RC = in.readInteger();
			if (RC == null)
				return null;
			int rc = RC.intValue();
			if (rc == RLSStatusCode.RLS_SUCCESS)
				return new MappingResult(rc, in.readString(), in.readString());
			if (rc == RLSStatusCode.RLS_LFN_NEXIST)
				return new MappingResult(rc, in.readString(), null);
			if (rc == RLSStatusCode.RLS_PFN_NEXIST)
				return new MappingResult(rc, null, in.readString());
			throw new RPCDeserializationException("Unexpected result code " +
					"value returned from stream (rc: " + rc + ")");
		}
	}
	MappingResultDeserializer mappingResultDeserializer = 
		new MappingResultDeserializer();

	/** Reads bulk update mapping results: rc, logical/target */
	class MappingUpdateDeserializer implements RPCObjectDeserializer {

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			Integer RC = in.readInteger();
			if (RC == null)
				return null;
			int rc = RC.intValue();
			return new MappingResult(rc, in.readString(), in.readString());
		}
	}
	MappingUpdateDeserializer mappingUpdateDeserializer = 
		new MappingUpdateDeserializer();

	/** Reads bulk exist mapping results: rc, logical/target */
	class MappingExistsDeserializer implements RPCObjectDeserializer {

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			Integer RC = in.readInteger();
			if (RC == null)
				return null;
			int rc = RC.intValue();
			if (rc == RLSStatusCode.RLS_SUCCESS)
				return new MappingResult(rc, in.readString(), in.readString());
			if (rc == RLSStatusCode.RLS_LFN_NEXIST)
				return new MappingResult(rc, in.readString(), null);
			if (rc == RLSStatusCode.RLS_PFN_NEXIST)
				return new MappingResult(rc, null, in.readString());
			throw new RPCDeserializationException("Unexpected result code " +
					"value returned from stream (rc: " + rc + ")");
		}
	}
	MappingExistsDeserializer mappingExistsDeserializer = 
		new MappingExistsDeserializer();

	/** Reads logical-target name pairs. */
	class SimpleMappingResultDeserializer implements RPCObjectDeserializer {

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			String logical, target;
			
			// Read logical
			logical = in.readString();
			if (logical == null)
				return null;
			
			// Check for continuation marker
			byte[] b = logical.getBytes();
			if (b[0] == 1 && b.length == 1)
				return new ResultsContinuationMarker();
			
			// Read target
			target = in.readString();
			if (target == null)
				throw new RPCDeserializationException(
						"Expected logical-target string pair but only found a " +
						"single string");
			return new MappingResult(RLSStatusCode.RLS_SUCCESS, logical, target);
		}
	}
	SimpleMappingResultDeserializer simpleMappingResultDeserializer = 
		new SimpleMappingResultDeserializer();

	/** Reads bulk index mapping results: rc, logical/catalog */
	class IndexMappingResultDeserializer implements RPCObjectDeserializer {

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			Integer RC = in.readInteger();
			if (RC == null)
				return null;
			int rc = RC.intValue();
			if (rc == RLSStatusCode.RLS_SUCCESS)
				return new IndexMappingResult(rc, in.readString(), in.readString());
			if (rc == RLSStatusCode.RLS_LFN_NEXIST)
				return new IndexMappingResult(rc, in.readString(), null);
			if (rc == RLSStatusCode.RLS_LRC_NEXIST)
				return new IndexMappingResult(rc, null, in.readString());
			throw new RPCDeserializationException("Unexpected result code " +
					"value returned from stream (rc: " + rc + ")");
		}
	}
	IndexMappingResultDeserializer indexMappingResultDeserializer = 
		new IndexMappingResultDeserializer();

	/** Reads bulk index mapping results: rc, logical/catalog */
	class IndexMappingExistsDeserializer implements RPCObjectDeserializer {

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			Integer RC = in.readInteger();
			if (RC == null)
				return null;
			int rc = RC.intValue();
			if (rc == RLSStatusCode.RLS_SUCCESS)
				return new IndexMappingResult(rc, in.readString(), null);
			if (rc == RLSStatusCode.RLS_LFN_NEXIST)
				return new IndexMappingResult(rc, in.readString(), null);
			if (rc == RLSStatusCode.RLS_LRC_NEXIST)
				return new IndexMappingResult(rc, null, in.readString());
			throw new RPCDeserializationException("Unexpected result code " +
					"value returned from stream (rc: " + rc + ")");
		}
	}
	IndexMappingExistsDeserializer indexMappingExistsDeserializer = 
		new IndexMappingExistsDeserializer();

	/** Reads logical-catalog string pairs. */
	class SimpleIndexMappingResultDeserializer implements RPCObjectDeserializer {

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			String logical, catalog;
			
			// Read logical
			logical = in.readString();
			if (logical == null)
				return null;
			
			// Check for continuation marker
			byte[] b = logical.getBytes();
			if (b[0] == 1 && b.length == 1)
				return new ResultsContinuationMarker();
			
			// Read catalog
			catalog = in.readString();
			if (catalog == null)
				throw new RPCDeserializationException(
						"Expected logical-catalog string pair but only found a " +
						"single string");
			return new IndexMappingResult(
					RLSStatusCode.RLS_SUCCESS, logical, catalog);
		}
	}
	SimpleIndexMappingResultDeserializer simpleIndexMappingResultDeserializer = 
		new SimpleIndexMappingResultDeserializer();

	/** Reads bulk rename results: rc, from/to */
	class RenameResultDeserializer implements RPCObjectDeserializer {

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			Integer RC = in.readInteger();
			if (RC == null)
				return null;
			int rc = RC.intValue();
			return new RenameResult(rc, in.readString(), in.readString());
		}
	}
	RenameResultDeserializer renameResultDeserializer = 
		new RenameResultDeserializer();

	/** Reads logical-target name pairs. */
	class RLIUpdateDeserializer implements RPCObjectDeserializer {

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			String url, pattern;
			
			// Read url
			url = in.readString();
			if (url == null)
				return null;
			
			// Read pattern. "Null" is valid value for pattern.
			pattern = in.readString();
			
			return new RLIUpdate(url, pattern);
		}
	}
	RLIUpdateDeserializer rliUpdateDeserializer = 
		new RLIUpdateDeserializer();

	/** Reads RLI Info. */
	class RLIInfoDeserializer implements RPCObjectDeserializer {

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			// Read url
			String url = in.readString();
			if (url == null)
				return null;
			
			// Read updateinterval
			Integer updateinterval = in.readInteger();
			if (updateinterval == null)
				throw new RPCDeserializationException("updateinterval is null");
			
			// Read flags
			Integer flags = in.readInteger();
			if (flags == null)
				throw new RPCDeserializationException("flags is null");
			
			// Read lastupdate
			Integer lastupdate = in.readInteger();
			if (lastupdate == null)
				throw new RPCDeserializationException("lastupdate is null");
			
			return new RLSRLIInfo(url, updateinterval.intValue(),
					flags.intValue(), lastupdate.intValue());
		}
	}
	RLIInfoDeserializer rliInfoDeserializer = 
		new RLIInfoDeserializer();

	/** Reads RLI Info. */
	class SenderInfoDeserializer implements RPCObjectDeserializer {

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			// Read url
			String url = in.readString();
			if (url == null)
				return null;
			
			// Read lastupdate
			Integer lastupdate = in.readInteger();
			if (lastupdate == null)
				throw new RPCDeserializationException("lastupdate is null");
			
			return new RLSLRCInfo(url, lastupdate.intValue());
		}
	}
	SenderInfoDeserializer senderInfoDeserializer = 
		new SenderInfoDeserializer();

	/** Reads RLS Stats. */
	class StatsDeserializer implements RPCObjectDeserializer {

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			// Read url
			String ver = in.readString();
			if (ver == null)
				return null;
			
			// Read uptime
			Integer uptime = in.readInteger();
			if (uptime == null)
				throw new RPCDeserializationException("uptime is null");
			
			// Read flags
			Integer flags = in.readInteger();
			if (flags == null)
				throw new RPCDeserializationException("flags is null");
			
			// Read bloomfilterui
			Integer bloomfilterui = in.readInteger();
			if (bloomfilterui == null)
				throw new RPCDeserializationException("bloomfilterui is null");
			
			// Read lfnlistui
			Integer lfnlistui = in.readInteger();
			if (lfnlistui == null)
				throw new RPCDeserializationException("lfnlistui is null");
			
			// Read numlfn
			Integer numlfn = in.readInteger();
			if (numlfn == null)
				throw new RPCDeserializationException("numlfn is null");
			
			// Read numpfn
			Integer numpfn = in.readInteger();
			if (numpfn == null)
				throw new RPCDeserializationException("numpfn is null");
			
			// Read nummap
			Integer nummap = in.readInteger();
			if (nummap == null)
				throw new RPCDeserializationException("nummap is null");
			
			// Read rli_numlfn
			Integer rli_numlfn = in.readInteger();
			if (rli_numlfn == null)
				throw new RPCDeserializationException("rli_numlfn is null");
			
			// Read rli_numlrc
			Integer rli_numlrc = in.readInteger();
			if (rli_numlrc == null)
				throw new RPCDeserializationException("rli_numlrc is null");
			
			// Read rli_numsender
			Integer rli_numsender = in.readInteger();
			if (rli_numsender == null)
				throw new RPCDeserializationException("rli_numsender is null");
			
			// Read rli_nummap
			Integer rli_nummap = in.readInteger();
			if (rli_nummap == null)
				throw new RPCDeserializationException("rli_nummap is null");
			
			return new RLSStats(ver, uptime.intValue(), flags.intValue(),
					bloomfilterui.intValue(), lfnlistui.intValue(), 
					numlfn.intValue(), numpfn.intValue(), nummap.intValue(),
					rli_numlfn.intValue(), rli_numlrc.intValue(),
					rli_numsender.intValue(), rli_nummap.intValue());
		}
	}
	StatsDeserializer statsDeserializer = 
		new StatsDeserializer();

	/** Reads Configuration Options. */
	class ConfigOptionDeserializer implements RPCObjectDeserializer {

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			// Read name
			String name = in.readString();
			if (name == null)
				return null;
			
			// Read value
			String value = in.readString();
			if (value == null)
				throw new RPCDeserializationException("value is null");
			
			return new ConfigurationOption(name, value);
		}
	}
	ConfigOptionDeserializer configOptionDeserializer = 
		new ConfigOptionDeserializer();

	/** Reads object name (i.e., logical or target name) and the attribute name. */
	class AttrObjResultDeserializer implements RPCObjectDeserializer {

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			Integer RC = in.readInteger();
			if (RC == null)
				return null;
			int rc = RC.intValue();
			if (rc != RLSStatusCode.RLS_SUCCESS)
				return new AttributeResult(rc, in.readString(), in.readString());
			else
				return null;
		}
	}
	AttrObjResultDeserializer attrObjResultDeserializer = 
		new AttrObjResultDeserializer();

	/** Deseializes attribute value. */
	abstract class AttrValDeserializer implements RPCObjectDeserializer {

		public RLSAttribute deserializeAttribute(RPCInputStream in, String name,
				int objtype, int valtype) throws IOException, RPCDeserializationException {
			RLSAttribute attr;
			switch (valtype) {
			case RLSAttribute.STR:
				attr = new RLSAttribute(name, objtype, in.readString());
				break;
			case RLSAttribute.INT:
				attr = new RLSAttribute(name, objtype, valtype, in.readInt());
				break;
			case RLSAttribute.DOUBLE:
				attr = new RLSAttribute(name, objtype, in.readDouble());
				break;
			case RLSAttribute.DATE:
				attr = new RLSAttribute(name, objtype, in.readDate());
				break;
			default:
				throw new RPCDeserializationException(
						"Invalid attribute value type: " + valtype);
			}
			return attr;
		}
	}

	/** Reads object name (i.e., logical or target name) and the attribute name. */
	class AttrValGetResultDeserializer extends AttrValDeserializer {
		
		private int objtype;
		
		AttrValGetResultDeserializer(int objtype) {
			this.objtype = objtype;
		}

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			Integer RC = in.readInteger();
			if (RC == null)
				return null;
			int rc = RC.intValue();
			if (rc != RLSStatusCode.RLS_SUCCESS) {
				String key = in.readString();
				String name = in.readString();
				return new RLSAttributeObject(
						rc, new RLSAttribute(name, objtype, -1), key);
			}
			else {
				String key = in.readString();
				String name = in.readString();
				int valtype = in.readInt();
				RLSAttribute attr = deserializeAttribute(in, name, -1, valtype);
				return new RLSAttributeObject(rc, attr, key);
			}
		}
	}

	/** Reads attribute search results. */
	class AttrSearchResultDeserializer extends AttrValDeserializer {
		
		private String name;
		private int objtype;
		
		AttrSearchResultDeserializer(String name, int objtype) {
			this.name = name;
			this.objtype = objtype;
		}

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			String key = in.readString();
			if (key == null)
				return null;
			int valtype = in.readInt();
			RLSAttribute attr = deserializeAttribute(in, name, objtype, valtype);
			return new RLSAttributeObject(attr, key);
		}
	}

	/** Reads attribute definitions. */
	class AttrDeserializer implements RPCObjectDeserializer {
		
		private int objtype;
		
		AttrDeserializer(int objtype) {
			this.objtype = objtype;
		}

		public Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException {
			String name = in.readString();
			if (name == null)
				return null;
			int valtype = in.readInt();
			return new RLSAttribute(name, objtype, valtype);
		}
	}

	
	// OTHER //
	
	/** A class used to indicate that the results continuation marker has been
	 * reached when reading results from a query using the offset/reslimit
	 * options.
	 */
	class ResultsContinuationMarker {
	}
}
