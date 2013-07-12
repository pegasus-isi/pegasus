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

public class MappingResult extends Mapping {

	protected int rc = 0;

	public MappingResult(int rc, String logical, String target) {
		super(logical, target);
		this.rc = rc;
	}

	public MappingResult(int rc, Mapping map) {
		super(map);
		this.rc = rc;
	}

	public int getRC() {
		return this.rc;
	}
	
	public String toString() {
		return super.toString() + " [rc: " + this.rc + "]";
	}
}
