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

import org.ietf.jgss.GSSCredential;
import org.globus.util.GlobusURL;

/**
 * A connection source for RLS connection. Use a connection source to establish
 * a connection to an RLS.
 */
public interface RLSConnectionSource {

	/**
	 * Connect to an RLS server using the default settings for the connection
	 * source.
	 * @return A connection to a RLS server.
	 * @throws RLSException
	 */
	public RLSConnection connect() throws RLSException;
	
	/**
	 * Connect to an RLS server using the given url and credential.
	 * @param url The url of the RLS server.
	 * @param credential The initiator's security credential.
	 * @return A connection to a RLS server.
	 * @throws RLSException
	 */
	public RLSConnection connect(GlobusURL url, GSSCredential credential) 
		throws RLSException;
}
