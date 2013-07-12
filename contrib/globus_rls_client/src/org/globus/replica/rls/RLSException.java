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

public class RLSException extends java.lang.Exception {

	private int rc = 0;

	public RLSException(int rc) {
		super(RLSStatusCode.toMessage(rc));
		this.rc = rc;
	}

	public RLSException(int rc, String msg) {
		super(RLSStatusCode.toMessage(rc) + ": " + msg);
		this.rc = rc;
	}
	
	public RLSException(int rc, String msg, Throwable cause) {
		super(RLSStatusCode.toMessage(rc) + ": " + msg, cause);
		this.rc = rc;
	}
	
	public RLSException(int rc, Throwable cause) {
		super(RLSStatusCode.toMessage(rc), cause);
		this.rc = rc;
	}

	public RLSException(Throwable cause) {
		super(cause);
	}

	/**
	 * @deprecated Please use {@link org.globus.replica.rls.RLSException#getRC()}.
	 * @return The RLS return code.
	 */
	public int GetRC() {
		return this.rc;
	}

	/**
	 * @return The RLS return code.
	 */
	public int getRC() {
		return this.rc;
	}
}
