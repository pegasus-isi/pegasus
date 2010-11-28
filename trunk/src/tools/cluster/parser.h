/**
 *  Copyright 2007-2010 University Of Southern California
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

#ifndef _PARSER_H
#define _PARSER_H

extern
size_t
interpreteArguments( char* cmd, char*** argv );
/* purpose: removes one layer of quoting and escaping, shell-style
 * paramtr: cmd (IO): commandline to split
 * paramtr: argv (OUT): argv[] vector, newly allocated vector
 * returns: argc 
 */

#endif /* _PARSER_H */
