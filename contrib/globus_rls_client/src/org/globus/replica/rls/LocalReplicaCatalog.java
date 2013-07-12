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
 * The <code>LocalReplicaCatalog</code> defines the interface for interacting
 * with the RLS Local Replica Catalog (LRC) service.
 */
public interface LocalReplicaCatalog {

	/**
	 * Create logical-target name mappings.
	 * 
	 * @param mappings
	 *            List of <code>Mapping</code> elements.
	 * 
	 * @return List of failed updates. Elements of <code>MappingResult</code>
	 *         type.
	 */
	public List createMappings(List mappings) throws RLSException;

	/**
	 * Delete logical-target name mappings.
	 * 
	 * @param mappings
	 *            List of <code>Mapping</code> elements.
	 * 
	 * @return List of failed updates. Elements will be of type
	 *         <code>MappingResult</code>.
	 */
	public List deleteMappings(List mappings) throws RLSException;

	/**
	 * Add logical-target name mappings to existing logical names.
	 * 
	 * @param List
	 *            List of <code>Mapping</code> elements. The logical name must
	 *            exist in the catalog. The target name may exist in the
	 *            catalog. The mapping from logical-target name must not exist.
	 * 
	 * @return List of failed updates. Elements will be of type
	 *         <code>MappingResult</code>.
	 */
	public List addMappings(List mappings) throws RLSException;

	/**
	 * Define new attributes in catalog.
	 * 
	 * @param attributes
	 *            List of <code>RLSAttribute</code> elements. Required fields
	 *            of name, objtype, and valtype must be properly initialized.
	 *            See
	 *            {@link org.globus.replica.rls.RLSAttribute attribute types}.
	 * 
	 * @return List of failed updates. Elements will be of type
	 *         <code>RLSAttributeObject</code>.
	 */
	public List defineAttributes(List attributes) throws RLSException;

	/**
	 * Undefine attributes in catalog.
	 * 
	 * @param attributes
	 *            List of <code>RLSAttribute</code> elements. Required fields
	 *            of name and objtype must be properly initialized. See
	 *            {@link org.globus.replica.rls.RLSAttribute attribute types}.
	 * 
	 * @return List of failed updates. Elements will be of type
	 *         <code>RLSAttributeObject</code>.
	 * 
	 * @param clearvalues
	 *            If true then any any values for this attribute are first
	 *            removed from the objects they're associated with. If false and
	 *            any values exist then an exception is thrown.
	 * 
	 * @return List of failed updates. Elements will be of type
	 *         <code>RLSAttributeObject</code>.
	 */
	public List undefineAttributes(List attributes, boolean clearvalues)
			throws RLSException;

	/**
	 * Add attributes to objects in the catalog.
	 * 
	 * @param attributes
	 *            List of attributes to add to objects. Elements must be of type
	 *            <code>RLSAttributeObject</code>.
	 * 
	 * @return List of failed updates. List elements will be of type
	 *         <code>AttributeResult</code>.
	 */
	public List addAttributes(List attributes) throws RLSException;

	/**
	 * Modify attribute values.
	 * 
	 * @param attributes
	 *            List of attributes to modify. Elements must be of type
	 *            <code>RLSAttributeObject</code>. The key, name, objtype,
	 *            and valtype must match an existing attribute. The value of the
	 *            attribute will be updated according to the new parameter
	 *            value.
	 * 
	 * @return List of failed updates. List elements will be of type
	 *         <code>AttributeResult</code>.
	 */
	public List modifyAttributes(List attributes) throws RLSException;

	/**
	 * Remove attributes from the catalog.
	 * 
	 * @param attributes
	 *            List of attributes to remove. Elements must be of type
	 *            <code>RLSAttributeObject</code>. The key, name, objtype,
	 *            and valtype must match an existing attribute.
	 * 
	 * @return List of failed updates. List elements will be of type
	 *         <code>AttributeResult</code>.
	 */
	public List removeAttributes(List attributes) throws RLSException;

	/**
	 * Rename logical names.
	 * 
	 * @param renames
	 *            List of logical names to rename. Elements of type
	 *            <code>Rename</code>, where <code>from</code> is an
	 *            existing name, and <code>to</code> is the new name.
	 * 
	 * @return List of failed updates. List elements will be of type
	 *         <code>RenameResult</code>.
	 */
	public List renameLogicalNames(List renames) throws RLSException;

	/**
	 * Rename target names.
	 * 
	 * @param renames
	 *            List of target names to rename. Elements of type
	 *            <code>Rename</code>, where <code>from</code> is an
	 *            existing name, and <code>to</code> is the new name.
	 * 
	 * @return List of failed updates. List elements will be of type
	 *         <code>RenameResult</code>.
	 */
	public List renameTargetNames(List renames) throws RLSException;

	/**
	 * Query catalog entries.
	 */
	public Results query(CatalogQuery query) throws RLSException;
}
