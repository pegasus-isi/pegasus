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
 * Queries that may return large result lists may use excessive server or client
 * resources (e.g. memory). RLS supports both server and client side limits on
 * the number of results returned by a query. The following class is used to
 * retrieve results incrementally. If clients do not specify an offset and limit
 * using this class then the API will retrieve all results on behalf of the
 * client. Alternatively the client can pass one of the following objects to the
 * query function and loop through the results a chunk at a time. The following
 * are equivlent:
 *  // Get all results list = rls.LRCGetPFNWC("*"); // process results
 *  // Get results 5 at a time RLSOffsetLimit offlim = newRLSOffsetLimit(0,5);
 * while (true) { list = rls.LRCGetLFNWC("*", offlim); // process results if
 * (offlim.offset == -1) // offset is set to -1 when no more results break; }
 */
public class RLSOffsetLimit {
	public int offset;

	public int reslimit;

	public RLSOffsetLimit() {
		offset = 0;
		reslimit = 0;
	}

	public RLSOffsetLimit(int ioffset, int ireslimit) {
		offset = ioffset;
		reslimit = ireslimit;
	}
}
