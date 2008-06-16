/**
 *  Copyright 2007-2008 University Of Southern California
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

/**
 * <p>Title: Pegasus</p>
 * <p>Description: </p>
 * <p>Copyright: Copyright (c) 2003</p>
 * <p>Company: ISI/USC</p>
 * @author Jens Vöckler
 * @version 1.0
 */


package org.griphyn.cPlanner.classes;

import java.io.LineNumberReader;

/**
 * This class is used to signal errors while scanning or parsing.
 * @see org.griphyn.cPlanner.classes.PoolConfigScanner
 * @see org.griphyn.cPlanner.classes.PoolConfigParser2
 */
public class PoolConfigException
  extends java.lang.RuntimeException
{
  private int m_lineno;

  public PoolConfigException( LineNumberReader stream, String message )
  {
    super("line " + stream.getLineNumber() + ": " + message);
    this.m_lineno = stream.getLineNumber();
  }

  public PoolConfigException( int lineno, String message )
  {
    super("line " + lineno + ": " + message);
    this.m_lineno = lineno;
  }

  public int getLineNumber()
  {
    return this.m_lineno;
  }
}
