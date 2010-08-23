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

#ifndef _MYSYSTEM_H
#define _MYSYSTEM_H

extern
int
mysystem( char* argv[], char* envp[], const char* special );
/* purpose: implement system(3c) call w/o calling the shell
 * paramtr: argv (IN): NULL terminated argument vector
 *          envp (IN): NULL terminated environment vector
 *          special (IN): set for setup/cleanup jobs
 * returns: exit status from wait() family 
 */

#endif /* _MYSYSTEM_H */
