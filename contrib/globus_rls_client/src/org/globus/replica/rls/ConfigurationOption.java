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
 * Represents a configuration option (name, value pair).
 */
public class ConfigurationOption {

	protected String name;

	protected String value;

	/**
	 * Construction a configuration option.
	 * 
	 * @param name
	 *            The name of the option.
	 * @param value
	 *            The value of the option.
	 */
	public ConfigurationOption(String name, String value) {
		this.name = name;
		this.value = value;
	}

	/**
	 * Returns the name of the option.
	 * 
	 * @return The name.
	 */
	public String getName() {
		return this.name;
	}

	/**
	 * Return the value of the option as a <code>String</code>.
	 * 
	 * @return The value.
	 */
	public String getValue() {
		return this.value;
	}
}
