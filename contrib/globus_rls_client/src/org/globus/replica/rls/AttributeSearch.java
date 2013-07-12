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
 * This class is used to express the attribute search style of query supported
 * by the RLS Server.
 */
public class AttributeSearch implements CatalogQuery {

	/**
	 * The <code>attributeSearch</code> is the only query type supported by this
	 * query object.
	 */
	public final static Integer attributeSearch = new Integer(1);

	protected Integer type;

	protected String name;

	protected int objtype;

	protected int op;

	protected RLSAttribute op1;

	protected RLSAttribute op2;

	protected RLSOffsetLimit offsetLimit;

	/**
	 * Search for objects (logical or target names) in a LRC service that have
	 * the specified attribute whose value matches a boolean expression. When
	 * using this query the catalog returns a <code>RLSAttributeObject</code>
	 * list. Any objects with the specified attribute will be returned along
	 * with the attribute value. See
	 * {@link org.globus.replica.rls.RLSAttributeObject RLSAttributeObject}.
	 * 
	 * @param name
	 *            Name of attribute.
	 * 
	 * @param objtype
	 *            Object (logical or target) type that attribute applies to.
	 * 
	 * @param op
	 *            Operator to be used in searching for values. See
	 *            {@link org.globus.replica.rls.RLSAttribute#OPALL operators}.
	 * 
	 * @param operand1
	 *            First operand in boolean expression.
	 * 
	 * @param operand2
	 *            Second operand in boolean expression, only used when op is
	 *            {@link org.globus.replica.rls.RLSAttribute#OPBTW RLSAttribute.OPBTW}.
	 * 
	 * @param offlim
	 *            Offset and limit used to retrieve results incrementally. Use
	 *            null, or 0,0, to retrieve all results.
	 */
	public AttributeSearch(String name, int objtype, int op, RLSAttribute op1,
			RLSAttribute op2, RLSOffsetLimit offsetLimit) {
		this.type = AttributeSearch.attributeSearch;
		this.name = name;
		this.objtype = objtype;
		this.op = op;
		this.op1 = op1;
		this.op2 = op2;
		this.offsetLimit = offsetLimit;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public int getObjtype() {
		return objtype;
	}

	public void setObjtype(int objtype) {
		this.objtype = objtype;
	}

	public RLSOffsetLimit getOffsetLimit() {
		return offsetLimit;
	}

	public void setOffsetLimit(RLSOffsetLimit offsetLimit) {
		this.offsetLimit = offsetLimit;
	}

	public int getOp() {
		return op;
	}

	public void setOp(int op) {
		this.op = op;
	}

	public RLSAttribute getOp1() {
		return op1;
	}

	public void setOp1(RLSAttribute op1) {
		this.op1 = op1;
	}

	public RLSAttribute getOp2() {
		return op2;
	}

	public void setOp2(RLSAttribute op2) {
		this.op2 = op2;
	}
}