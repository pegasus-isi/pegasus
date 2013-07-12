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

public class RLSStats {

	// Following constants must match defines in globus_rls_client.h.
	public static final int RLS_LRCSERVER = 0x1;

	public static final int RLS_RLISERVER = 0x2;

	public static final int RLS_RCVLFNLIST = 0x4;

	public static final int RLS_RCVBLOOMFILTER = 0x8;

	public static final int RLS_SNDLFNLIST = 0x10;

	public static final int RLS_SNDBLOOMFILTER = 0x20;

	/** New flag introduced for GT 5 streamline startup enhancement */
	public static final int RLS_INITIALIZED = 0x40;

	public String Version;

	public int Uptime;

	public int Flags;

	public int LRCBloomFilterUI;

	public int LRCLFNListUI;

	public int LRCNumLFN;

	public int LRCNumPFN;

	public int LRCNumMAP;

	public int RLINumLFN;

	public int RLINumLRC;

	public int RLINumSender;

	public int RLINumMAP;

	public RLSStats(String version, int uptime, int flags,
			int lrcbloomfilterui, int lrclfnlistui, int lrcnumlfn,
			int lrcnumpfn, int lrcnummap, int rlinumlfn, int rlinumlrc,
			int rlinumsender, int rlinummap) {
		Version = version;
		Uptime = uptime;
		Flags = flags;
		LRCBloomFilterUI = lrcbloomfilterui;
		LRCLFNListUI = lrclfnlistui;
		LRCNumLFN = lrcnumlfn;
		LRCNumPFN = lrcnumpfn;
		LRCNumMAP = lrcnummap;
		RLINumLFN = rlinumlfn;
		RLINumLRC = rlinumlrc;
		RLINumSender = rlinumsender;
		RLINumMAP = rlinummap;
	}

}
