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
 * Class to capture the content within a quoted string.
 */
class PoolConfigQuotedString
  implements PoolConfigToken
{
  /**
   * This instance variable captures the token value for the quoted string.
   */
  private String m_value;

  /**
   * Initializes an instance of a quoted string.
   * @param tokenValue is the string content to remember.
   */
  public PoolConfigQuotedString( String tokenValue )
  {
    m_value = tokenValue;
  }

  /**
   * Obtains the token value of a given string token.
   * @return the token value.
   */
  public String getValue()
  {
    return this.m_value;
  }
}
