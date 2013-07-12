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
 * Query existence of object or mapping.
 */
public class IndexExistenceQuery extends BatchQuery implements IndexQuery {

	/**
	 * Query existence of mappings (logical to catalog URL) in the index. Batch
	 * must contain {@link org.globus.replica.rls.IndexMapping} elements. Query results will be of
	 * type {@link org.globus.replica.rls.IndexMappingResult}.
	 */
	public static final Integer mappingExists = new Integer(1);

	/**
	 * Query existence of objects (logical or catalog URLs). Batch must contain
	 * {@link java.lang.String} elements. Query results will be of type
	 * {@link org.globus.replica.rls.ExistenceResult}.
	 */
	public static final Integer objectExists = new Integer(2);

	/**
	 * Object type for logical name.
	 */

	public static final Integer logical = new Integer(RLSAttribute.RLI_LFN);

	/**
	 * Object type for catalog URL.
	 */
	public static final Integer catalog = new Integer(RLSAttribute.RLI_LRC);

	protected Integer objectType = null;

	public IndexExistenceQuery(Integer type, List batch, Integer objectType) {
		super(type, batch);
		this.objectType = objectType;
	}

	public IndexExistenceQuery(Integer type, List batch) {
		this(type, batch, null);
	}
	
	public Integer getObjectType() {
		return this.objectType;
	}
}
