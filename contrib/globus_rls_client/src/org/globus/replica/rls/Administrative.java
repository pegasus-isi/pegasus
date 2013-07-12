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

import java.util.List;

/**
 * Defines the administrative interface for communicating with the RLS server.
 * Operations of this interface require the ADMIN privilege.
 */
public interface Administrative {

	/**
	 * Pings the RLS server. If the method returns without exception then the
	 * server has been successfully pinged.
	 * 
	 * @throws RLSException
	 */
	public void ping() throws RLSException;

	/**
	 * Shutdown the RLS server. If the method returns without exception then the
	 * server has initiated its shutdown sequence.
	 * 
	 * @throws RLSException
	 */
	public void quit() throws RLSException;

	/**
	 * Instructs RLS server to send updates immediately. If the method returns
	 * without exception then the server has attempted to send updates.
	 * 
	 * @throws RLSException
	 */
	public void update() throws RLSException;
	
	/**
	 * Returns the statistics of the RLS server.
	 * 
	 * @return RLSStats The currents statistics of the RLS server. See {link
	 *         org.globus.replica.rls.RLSStats RLSStats}.
	 * @throws RLSException
	 */
	public RLSStats stats() throws RLSException;
	
	/**
	 * Returns the values for the specified configuration option. If the option
	 * is "all" then the entire configuration of the RLS server is returned.
	 * 
	 * @param option
	 *            The RLS server configuration option or "all" to return all
	 *            configuration values.
	 * @return A list of configuration option elements representing each name,
	 *         value pair. See
	 *         {@link org.globus.replica.rls.ConfigurationOption 
	 *         ConfigurationOption}.
	 * @throws RLSException
	 */
	public List getConfiguration(String option) throws RLSException;

	/**
	 * Sets a configuration option of the RLS server.
	 * 
	 * @param name
	 *            The name of the option.
	 * @param value
	 *            The value (as a <code>String</code>) of the option.
	 * @throws RLSException
	 */
	public void setConfiguration(String name, String value) throws RLSException;

	/**
	 * LRC servers send information about logical names in their catalog to 
	 * RLI servers as specified using the following function. Updates may be 
	 * partitioned among multiple RLIs by specifying one or more patterns for an
	 * RLI.
	 * 
	 * @param update Specifies the LRC to RLI update.
	 * 		The url:
	 *            URL of RLI server that LRC should send updates to.
	 * 		The flags: 
	 *            Should be zero or
	 *            {@link org.globus.replica.rls.RLSClient#RLIFLAG_BLOOMFILTER RLSClient.RLIFLAG_BLOOMFILTER}
	 *            if the RLI should be updated via Bloom filters.
	 * 		The pattern: 
	 *            If not NULL used to filter which LFNs are sent to rli_url.
	 *            Standard Unix wildcard characters (*, ?) may be used to do
	 *            wildcard matches. Patterns are ignored if Bloom filters are
	 *            used for updates.
	 */
	public void addUpdate(RLIUpdate update) throws RLSException;

	/**
	 * Delete an entry from the LRC to RLI/partition update list.
	 * 
	 * @param update Specifies the LRC to RLI update.
	 * 		The url:
	 *            URL of RLI server to remove from LRC partition list.
	 * 		The pattern:
	 *            If not null then only the specific rli_url/pattern is removed,
	 *            else all partition information for rli_url is removed.
	 */
	public void deleteUpdate(RLIUpdate update) throws RLSException;

	/**
	 * Get RLI update partitions from LRC server.
	 *
	 * @param update Specifies the LRC to RLI update.
	 * 		The url:
	 *            If not NULL identifies RLI that partition data will be
	 *            retrieved for. If null then all RLIs are retrieved.
	 * 
	 *		The pattern:
	 *            If not NULL returns only partitions with matching patterns,
	 *            otherwise all patterns are retrieved.
	 * 
	 * @return List of
	 *         {@link org.globus.replica.rls.RLIUpdate RLIUpdate} objects,
	 *         containing the RLI URLs and patterns (if any) updated by the LRC.
	 */
	public List listUpdatePartitions(RLIUpdate update) throws RLSException;

	/**
	 * Get info about RLI server updated by an LRC server.
	 * 
	 * @param rliurl
	 *            URL of RLI server to retrieve info for.
	 * 
	 * @return Information about the RLI server will be returned in an
	 *         {@link org.globus.replica.rls.RLSRLIInfo RLSRLIInfo} object.
	 */
	public RLSRLIInfo findUpdate(String rliurl) throws RLSException;

	/**
	 * Return URLs of RLIs that LRC sends updates to.
	 * 
	 * @return A list of RLSRLIInfo objects containing the RLI URLs updated by
	 *         this LRC.
	 */
	public List listUpdates() throws RLSException;
	

	/**
	 * Return URLs of LRCs that update this RLI.
	 * 
	 * @return A list of RLSLRCInfo objects containing the RLI URLs updated by
	 *         this LRC.
	 */
	public List updatedBy() throws RLSException;

	/**
	 * Return URLs of RLIs that RLI sends updates to.
	 * 
	 * @return A list of RLSRLIInfo objects containing the RLI URLs updated by
	 *         this RLI.
	 */
	public List listRliToRliUpdates() throws RLSException;
}
