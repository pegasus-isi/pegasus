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
 * Simple catalog query.
 */
public class SimpleCatalogQuery extends SimpleQuery implements CatalogQuery {

	/**
	 * Query mappings (logical to target names) by logical name. Query results
	 * will be of type {@link org.globus.replica.rls.MappingResult}.
	 */
	public static final Integer queryMappingsByLogicalName = new Integer(1);

	/**
	 * Query mappings (logical to target names) by target name. Query results
	 * will be of type {@link org.globus.replica.rls.MappingResult}.
	 */
	public static final Integer queryMappingsByTargetName = new Integer(2);

	/**
	 * Query mappings (logical to target names) by logical name wildcard
	 * pattern. Query results will be of type {@link org.globus.replica.rls.MappingResult}.
	 */
	public static final Integer queryMappingsByLogicalNamePattern = new Integer(3);

	/**
	 * Query mappings (logical to target names) by target name wildcard pattern.
	 * Query results will be of type {@link org.globus.replica.rls.MappingResult}.
	 */
	public static final Integer queryMappingsByTargetNamePattern = new Integer(4);

	/**
	 * Query attribute definitions by attribute name and object type. Input
	 * parameter must be of type {@link org.globus.replica.rls.RLSAttribute}. If
	 * the name is <code>null</code>, then all attributes of the given 
	 * <code>object type</code> will be returned. Results are returned as a list
	 * of {@link org.globus.replica.rls.RLSAttribute}s. Offset and result limit
	 * options are not supported.
	 */
	public static final Integer queryAttributeDefinitions = new Integer(5);

	/**
	 * Creates a simple catalog query.
	 * @param type The type of query as defined in this class
	 * @param param The input parameter for the query
	 * @param offsetlimit An optional offset limit for the query or null
	 */
	public SimpleCatalogQuery(Integer type, Object param,
			RLSOffsetLimit offsetlimit) {
		super(type, param, offsetlimit);
	}
}
