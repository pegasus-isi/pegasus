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
package org.globus.replica.rls.impl.rpc;

import java.io.IOException;

/** 
 * Defines interface of object serializers for writing to RPC streams.
 */
interface RPCObjectSerializer {
	/**
	 * Serializes the object to the output stream.
	 * @param out The rpc output stream
	 * @param obj The object to be serialized
	 * @throws IOException
	 */
	void serialize(RPCOutputStream out, Object obj) throws IOException;
	
	/**
	 * Performs an instanceof check to determine whether the input object is
	 * an instance of a valid class for this object serializer.
	 * @param obj The input object
	 * @return True if the object is a valid instance of a support class and
	 * 		False otherwise
	 */
	boolean isValidInstance(Object obj);
}
