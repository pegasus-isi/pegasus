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
 * Simple query that supports offset and limit modifiers.
 */
public class SimpleQuery implements Query {

	protected Integer type;

	protected Object param;

	protected RLSOffsetLimit offsetlimit;

	SimpleQuery(Integer type, Object param, RLSOffsetLimit offsetlimit) {
		this.type = type;
		this.param = param;
		this.offsetlimit = offsetlimit;
	}

	public Object getParam() {
		return this.param;
	}

	public Integer getType() {
		return this.type;
	}

	public RLSOffsetLimit getOffsetLimit() {
		return this.offsetlimit;
	}
}
