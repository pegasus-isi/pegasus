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

import java.util.*;

public class RLSAttribute {

	// Following constants must match enums in globus_rls_client.h.
	public static final int DATE = 0;

	public static final int DOUBLE = 1;

	public static final int INT = 2;

	public static final int STR = 3;

	public static final int LRC_LFN = 0;

	public static final int LRC_PFN = 1;

	public static final int RLI_LFN = 2;

	public static final int RLI_LRC = 3;

	public static final int OPALL = 0;

	public static final int OPEQ = 1;

	public static final int OPNE = 2;

	public static final int OPGT = 3;

	public static final int OPGE = 4;

	public static final int OPLT = 5;

	public static final int OPLE = 6;

	public static final int OPBTW = 7;

	public static final int OPLIKE = 8;

	public String name;

	public RLSAttribute(String aname, int lobjtype, int lvaltype) {
		name = aname;
		objtype = lobjtype;
		valtype = lvaltype;
	}

	public RLSAttribute(String aname, int lobjtype, Date val) {
		name = aname;
		objtype = lobjtype;
		valtype = DATE;
		dateval = val;
	}

	// If initializing a date then val is seconds since Jan 1, 1970.
	public RLSAttribute(String aname, int lobjtype, int lvaltype, int val) {
		name = aname;
		objtype = lobjtype;
		valtype = lvaltype;
		if (valtype == DATE)
			dateval = new Date((long) val * 1000);
		else
			intval = val;
	}

	public RLSAttribute(String aname, int lobjtype, double val) {
		name = aname;
		objtype = lobjtype;
		valtype = DOUBLE;
		doubleval = val;
	}

	public RLSAttribute(String aname, int lobjtype, String val) {
		name = aname;
		objtype = lobjtype;
		valtype = STR;
		strval = val;
	}

	public int GetObjType() {
		return objtype;
	}

	public int GetValType() {
		return valtype;
	}

	public String GetValTypeName() {
		switch (valtype) {
		case DATE:
			return "date";
		case DOUBLE:
			return "double";
		case INT:
			return "integer";
		case STR:
			return "string";
		}
		return null;
	}

	public Date GetDateVal() {
		return dateval;
	}

	public int GetDateValSeconds() {
		return (int) (dateval.getTime() / 1000);
	}

	public double GetDoubleVal() {
		return doubleval;
	}

	public int GetIntVal() {
		return intval;
	}

	public String GetStrVal() {
		return strval;
	}

	public String toString() {
		switch (valtype) {
		case DATE:
			return dateval.toString();
		case INT:
			return Integer.toString(intval);
		case DOUBLE:
			return Double.toString(doubleval);
		case STR:
			return strval;
		}
		return null;
	}

	private int valtype;

	private int objtype;

	private Date dateval;

	private double doubleval;

	private int intval;

	private String strval;
}
