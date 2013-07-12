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
 * Represents the result of an attribute update (add, remove, modify).
 */
public class AttributeResult {

	protected int rc;

	protected String key; // lfn/pfn

	protected String name; // attr key

	public AttributeResult(int rc, String key) {
		this.rc = rc;
		this.key = key;
	}

	public AttributeResult(int rc, String key, String name) {
		this.rc = rc;
		this.key = key;
		this.name = name;
	}

	public int getRC() {
		return this.rc;
	}

	public String getKey() {
		return this.key;
	}

	public String getName() {
		return this.name;
	}
}
