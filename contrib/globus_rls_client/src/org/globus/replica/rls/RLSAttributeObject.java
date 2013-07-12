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

public class RLSAttributeObject {
	public int rc;

	public RLSAttribute attr;

	public String key;

	public RLSAttributeObject(RLSAttribute lattr) {
		attr = lattr;
	}

	public RLSAttributeObject(RLSAttribute lattr, String lkey) {
		rc = 0;
		attr = lattr;
		key = lkey;
	}

	public RLSAttributeObject(int lrc, RLSAttribute lattr, String lkey) {
		rc = lrc;
		attr = lattr;
		key = lkey;
	}
}
