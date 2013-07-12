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
 * Used in rename operations to rename a logical or target name.
 */
public class Rename {

	protected String from;

	protected String to;

	public Rename(String from, String to) {
		this.from = from;
		this.to = to;
	}

	public String getFrom() {
		return this.from;
	}

	public String getTo() {
		return this.to;
	}
}
