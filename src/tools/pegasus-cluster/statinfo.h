/**
 *  Copyright 2007-2011 University Of Southern California
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
#ifndef _STATINFO_H
#define _STATINFO_H

extern
int
myaccess( const char* path );
/* purpose: check a given file for being accessible and executable
 *          under the currently effective user and group id. 
 * paramtr: path (IN): current path to check
 * returns: 0 if the file is accessible, -1 for not
 */

extern
char*
find_executable( const char* fn );
/* purpose: check the executable filename and correct it if necessary
 * paramtr: fn (IN): current knowledge of filename
 * returns: newly allocated fqpn of path to exectuble, or NULL if not found
 * globals: this will muck up the value in 'errno'. 
 */

#endif /* _STATINFO_H */
