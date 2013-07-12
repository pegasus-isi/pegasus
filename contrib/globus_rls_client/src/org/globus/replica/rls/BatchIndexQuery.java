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

import java.util.List;

/**
 * A basic index query. Support a batch of queries.
 */
public class BatchIndexQuery extends BatchQuery implements IndexQuery {

	/**
	 * Query mappings (logical to target names) by logical name. Batch must
	 * contain {@link java.lang.String} elements. Query results will be of type
	 * {@link org.globus.replica.rls.MappingResult}. The <code>target</code> 
	 * field of the mapping result object will be the URL to the catalog that 
	 * contains a reference to the logical name.
	 */
	public static final Integer queryMappingsByLogicalNames = new Integer(1);

	/**
	 * Query mappings (logical to target names) by logical name patterns
	 * (supports wildcards). Batch must contain {@link java.lang.String}
	 * elements. Query results will be of type 
	 * {@link org.globus.replica.rls.MappingResult}. The <code>target</code> 
	 * field of the mapping result object will be the URL to the catalog that 
	 * contains a reference to the logical name.
	 */
	public static final Integer queryMappingsByLogicalNamePatterns = 
		new Integer(2);

	public BatchIndexQuery(Integer type, List batch) {
		super(type, batch);
	}
}
