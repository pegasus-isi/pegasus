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
 * Represents a mapping from logical to target name.
 */
public class Mapping {

	protected String logical;

	protected String target;

	public Mapping(String logical, String target) {
		this.logical = logical;
		this.target = target;
	}

	public Mapping(Mapping copy) {
		this.logical = copy.logical;
		this.target = copy.target;
	}

	public String getLogical() {
		return this.logical;
	}

	public String getTarget() {
		return this.target;
	}

	public String toString() {
		return super.toString() + " [logical: " + this.logical + ", target: " +
				this.target + "]";
	}
}
