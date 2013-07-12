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
public class CatalogExistenceQuery extends BatchQuery implements CatalogQuery {

	/**
	 * Query existence of mappings (logical to target names) in the catalog or
	 * Batch must contain {@link org.globus.replica.rls.Mapping} elements. Query
	 *  results will be of type {@link org.globus.replica.rls.MappingResult}.
	 */
	public static final Integer mappingExists = new Integer(1);

	/**
	 * Query existence of objects (logical or target names). Batch must contain
	 * {@link java.lang.String} elements. Query results will be of type
	 * {@link org.globus.replica.rls.ExistenceResult}.
	 */
	public static final Integer objectExists = new Integer(2);

	/**
	 * Object type for logical name.
	 */

	public static final Integer logical = new Integer(RLSAttribute.LRC_LFN);

	/**
	 * Object type for target name.
	 */
	public static final Integer target = new Integer(RLSAttribute.LRC_PFN);

	protected Integer objectType = null;

	public CatalogExistenceQuery(Integer type, List batch, Integer objectType) {
		super(type, batch);
		this.objectType = objectType;
	}

	public CatalogExistenceQuery(Integer type, List batch) {
		this(type, batch, null);
	}

	public Integer getObjectType() {
		return objectType;
	}
}
