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

import java.util.*;

public class RLSLRCInfo {

	public String url;

	public Date lastupdate;

	public RLSLRCInfo() {
		url = null;
		lastupdate = new Date(0L);
	}

	public RLSLRCInfo(String lurl, int llastupdate) {
		url = lurl;
		lastupdate = new Date((long) llastupdate * 1000);
	}
}
