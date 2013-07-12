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
package org.globus.replica.rls.impl;

import org.ietf.jgss.GSSCredential;
import org.globus.replica.rls.RLSConnection;
import org.globus.replica.rls.RLSConnectionSource;
import org.globus.replica.rls.RLSException;
import org.globus.replica.rls.impl.rpc.SimpleRLSConnection;
import org.globus.util.GlobusURL;

/**
 * Basic implementation of a RLS connection source. Objects of this class may be
 * used as JNDI resources in a container environment.
 * @see org.globus.replica.rls.impl.SingletonRLSConnection
 */
public class SimpleRLSConnectionSource implements RLSConnectionSource {

	protected GlobusURL defaultUrl;
	protected GSSCredential defaultCredential;

	/**
	 * This call will work properly ONLY if the defaultUrl and defaultCredential
	 * fields have been initialized.
	 */
	public RLSConnection connect() throws RLSException {
		return new SimpleRLSConnection(this.defaultUrl, this.defaultCredential);
	}

	/**
	 * Establishes a connection based on the given url and credential, which
	 * override the defaults.
	 */
	public RLSConnection connect(GlobusURL url, GSSCredential credential) throws RLSException {
		return new SimpleRLSConnection(url, credential);
	}

	public GSSCredential getDefaultCredential() {
		return defaultCredential;
	}

	public void setDefaultCredential(GSSCredential defaultCredential) {
		this.defaultCredential = defaultCredential;
	}

	public GlobusURL getDefaultUrl() {
		return defaultUrl;
	}

	public void setDefaultUrl(GlobusURL defaultUrl) {
		this.defaultUrl = defaultUrl;
	}
}
