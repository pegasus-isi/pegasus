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
package org.globus.replica.rls.impl;

import org.globus.replica.rls.RLSConnectionSource;

/**
 * Use this if you want a singleton connection source rather than instantiating 
 * a simple connection source by another means. This singleton is an extension
 * of {@link org.globus.replica.rls.impl.SimpleRLSConnectionSource SimpleRLSConnectionSource}.
 * @see org.globus.replica.rls.impl.SimpleRLSConnection
 */
public class SingletonRLSConnectionSource extends SimpleRLSConnectionSource {

	private static SingletonRLSConnectionSource instance = null;

	/**
	 * The first call to this method will instantiate a new instance of the
	 * class. It does not initialize the object's fields. The caller should
	 * initialize the object properly.
	 */
	public static RLSConnectionSource instance() {
		if (SingletonRLSConnectionSource.instance == null)
			SingletonRLSConnectionSource.instance = new SingletonRLSConnectionSource();

		return SingletonRLSConnectionSource.instance;
	}
}
