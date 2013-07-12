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

/**
 * Represents a connection to the RLS server.
 */
public interface RLSConnection {

	/**
	 * Returns the catalog interface.
	 * @return interface to the catalog
	 */
	public LocalReplicaCatalog catalog();

	/**
	 * Returns the index interface.
	 * @return interface to the index
	 */
	public ReplicaLocationIndex index();

	/**
	 * Returns the administrative interface.
	 * @return the interface for administration
	 */
	public Administrative admin();

	/**
	 * Closes the RLS connection.
	 */
	public void close() throws org.globus.replica.rls.RLSException;
}
