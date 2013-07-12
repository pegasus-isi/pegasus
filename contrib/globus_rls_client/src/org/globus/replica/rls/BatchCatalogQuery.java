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
 * A basic catalog query. Supports a batch of queries.
 */
public class BatchCatalogQuery extends BatchQuery implements CatalogQuery {

	/*
	 * Query definitions of attributes in the catalog. Batch must contain
	 * <code>RLSAttribute</code> elements with name and objtype properly
	 * initialized. Query results will be of type <code>RLSAttribute</code>.
	 * See {@link org.globus.replica.rls.RLSAttribute RLSAttribute}.
	 *
	public static final Integer attributeDefinitionQuery = new Integer(1);
	*/

	/**
	 * Query attributes in catalog for specified object (logical or target
	 * name). Batch must contain <code>RLSAttributeObject</code> elements with
	 * key and objtype properly initialized. The attribute's name field may be
	 * specified to narrow the query. Query results will be of type
	 * <code>RLSAttributeObject</code>. See
	 * {@link org.globus.replica.rls.RLSAttributeObject RLSAttributeObject}.
	 */
	public static final Integer attributeQuery = new Integer(3);

	/**
	 * Query mappings (logical to target names) by logical name. Batch must
	 * contain <code>String</code> elements. Query results will be of type
	 * <code>MappingResult</code>.
	 */
	public static final Integer mappingQueryByLogicalNames = new Integer(4);

	/**
	 * Query mappings (logical to target names) by target name. Batch must
	 * contain <code>String</code> elements. Query results will be of type
	 * <code>MappingResult</code>.
	 */
	public static final Integer mappingQueryByTargetNames = new Integer(5);

	/**
	 * Base constructor for batch catalog queries.
	 * 
	 * @param type
	 *            The type of the query.
	 * @param batch
	 *            The batch input parameters.
	 */
	public BatchCatalogQuery(Integer type, List batch) {
		super(type, batch);
	}

	/**
	 * The attribute name for the attributeQuery.
	 */
	protected String attributeName;

	/**
	 * The object type for the attributeQuery.
	 */
	protected Integer attributeObjectType;

	/**
	 * Constructor for bulk attribute queries.
	 * 
	 * @param keylist
	 *            The list of (logical or target) names.
	 * @param name
	 *            The attribute name.
	 * @param objtype
	 *            The object type (logical or target) of the keys.
	 */
	public BatchCatalogQuery(List keylist, String name, Integer objtype) {
		super(BatchCatalogQuery.attributeQuery, keylist);
		this.attributeName = name;
		this.attributeObjectType = objtype;
	}

	public String getAttributeName() {
		return attributeName;
	}

	public Integer getAttributeObjectType() {
		return attributeObjectType;
	}
}
