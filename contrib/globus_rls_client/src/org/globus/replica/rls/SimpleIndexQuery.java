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
 * Simple index query.
 */
public class SimpleIndexQuery extends SimpleQuery implements IndexQuery {

	/**
	 * Query mappings (logical name to catalog URL) by logical name. Query
	 * results will be of type {@link org.globus.replica.rls.IndexMappingResult}.
	 */
	public static final Integer queryMappingsByLogicalName = 
		new Integer(1);

	/**
	 * Query mappings (logical name to catalog URL) by logical name wildcard
	 * pattern. Query results will be of type 
	 * {@link org.globus.replica.rls.IndexMappingResult}.
	 */
	public static final Integer queryMappingsByLogicalNamePattern = 
		new Integer(2);

	/**
	 * Creates a simple index query.
	 * @param type One of the query types defined in this class
	 * @param param The input parameter for the query
	 * @param offsetlimit Optional offset limit for the query or null
	 */
	public SimpleIndexQuery(Integer type, String param, RLSOffsetLimit offsetlimit) {
		super(type, param, offsetlimit);
	}

}
