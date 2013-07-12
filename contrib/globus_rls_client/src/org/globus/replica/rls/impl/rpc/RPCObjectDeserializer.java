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
 * Defines inteface for object deserialization from an RPC input stream.
 */
interface RPCObjectDeserializer {
	/**
	 * Deserializes an object from an input stream.
	 * @param in The rpc input stream
	 * @return The object deserialized from the input stream or null if end of 
	 * 		stream reached
	 * @throws IOException I/O failure
	 * @throws RPCDeserializationException Failed to deserialize object from
	 * 		stream
	 */
	Object deserialize(RPCInputStream in) throws IOException, RPCDeserializationException;
}
