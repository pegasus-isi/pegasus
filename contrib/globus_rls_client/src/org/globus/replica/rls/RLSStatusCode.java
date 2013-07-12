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
 * The <code>RLSStatusCode</class> class contains a listing of the status codes
 * used by the RLS Server. These codes must match the RLS header file
 * <code>globus_rls_client.h</code>.
 */
public final class RLSStatusCode {

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

	private static final String errmsg[] = {
	    "No error",
	    "Globus I/O error", // Consider calling this "internal error" instead
	    "Invalid handle",
	    "Bad URL",
	    "No memory",
	    "Buffer overflow",
	    "Bad argument",
	    "Permission denied",
	    "Bad method name",
	    "Invalid server",
	    "Mapping doesn't exist",
	    "LFN already exists",
	    "LFN doesn't exist",
	    "PFN already exists",
	    "PFN doesn't exist",
	    "LRC already exists",
	    "LRC doesn't exist",
	    "DB error",
	    "RLI already exists",
	    "RLI doesn't exist",
	    "Mapping already exists",
	    "Invalid attribute type",
	    "Attribute already exists",
	    "Attribute doesn't exist",
	    "Invalid object type",
	    "Invalid operator for attribute search",
	    "Operation is unsupported",
	    "IO timeout",
	    "Too many connections",
	    "Attribute with specified value doesn't exist",
	    "Attribute still referenced by objects in database"
	};
	
	/**
	 * Converts the given status code into the associated textual message.
	 * @param rc The status code.
	 * @return The textual message associated with the status code.
	 */
	public static String toMessage(int rc) {
		return (rc>=0 && rc<errmsg.length) ? errmsg[rc] : "Invalid RC ("+rc+")";
	}
}
