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

package org.griphyn.cPlanner.classes;

/**
 * Class to capture reserved words.
 * $Revision$
 * @author Jens Vöckler
 * @author Gaurang Mehta
 */
class PoolConfigReservedWord
  implements PoolConfigToken
{
  /**
   * token value for the reserved word "site".
   */
  public static final int SITE = 0;

  /**
   * token value for the reserved word "version".
   */
  public static final int VERSION = 1;

  /**
   * token value for the reserved word "lrc".
   */
  public static final int LRC = 2;

  /**
   * token value for the reserved word "universe".
   */
  public static final int UNIVERSE = 3;

  /**
   * token value for the reserved word "gridlaunch".
   */
  public static final int GRIDLAUNCH = 4;

  /**
   * token value for the reserved word "workdir".
   */
  public static final int WORKDIR = 5;

  /**
   * token value for the reserved word "gridftp".
   */
  public static final int GRIDFTP = 6;

  /**
   * token value for the reserver word "profile".
   */

  public static final int PROFILE = 7;

  /**
   * token value for the reserved work sysinfo.
   */

   public static final int SYSINFO = 8;

  /**
   * Singleton implementation of a symbol table for reserved words.
   */
  private static java.util.Map m_symbolTable = null;

  /**
   * Singleton access to the symbol table as a whole.
   * @return Map
   */
  public static java.util.Map symbolTable()
  {
    if ( m_symbolTable == null ) {
      // only initialize once and only once, as needed.
      m_symbolTable = new java.util.TreeMap();
      m_symbolTable.put( "site",
	 new PoolConfigReservedWord(PoolConfigReservedWord.SITE) );
      m_symbolTable.put( "version",
	 new PoolConfigReservedWord(PoolConfigReservedWord.VERSION) );
      m_symbolTable.put( "lrc",
	 new PoolConfigReservedWord(PoolConfigReservedWord.LRC) );
      m_symbolTable.put( "universe",
	 new PoolConfigReservedWord(PoolConfigReservedWord.UNIVERSE) );
      m_symbolTable.put( "gridlaunch",
	 new PoolConfigReservedWord(PoolConfigReservedWord.GRIDLAUNCH) );
      m_symbolTable.put( "workdir",
	 new PoolConfigReservedWord(PoolConfigReservedWord.WORKDIR) );
      m_symbolTable.put( "gridftp",
                         new PoolConfigReservedWord(PoolConfigReservedWord.GRIDFTP) );
      m_symbolTable.put( "profile",
                         new PoolConfigReservedWord(PoolConfigReservedWord.PROFILE) );
      m_symbolTable.put("sysinfo",
                        new PoolConfigReservedWord(PoolConfigReservedWord.SYSINFO));
    }

    return m_symbolTable;
  }

  /**
   * This instance variable captures the token value for the reserved word.
   */
  private int m_value;

  /**
   * Initializes an instance of a reserved word token. The constructor
   * is unreachable from the outside. Use symbol table lookups to obtain
   * reserved word tokens.
   * @param tokenValue is the token value to memorize.
   * @see #symbolTable()
   */
  protected PoolConfigReservedWord( int tokenValue )
  {
    m_value = tokenValue;
  }

  /**
   * Obtains the token value of a given reserved word token.
   * @return the token value.
   */
  public int getValue()
  {
    return this.m_value;
  }
}
