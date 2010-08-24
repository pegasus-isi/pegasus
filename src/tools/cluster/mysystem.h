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

typedef struct {
  struct sigaction intr;
  struct sigaction quit;
  sigset_t         mask; 
} Signals;

extern
int
save_signals( Signals* save );
/* purpose: ignore SIG{INT,QUIT} and block SIGCHLD
 * paramtr: save (IO): place to store original signal vectors
 * returns: 0 on success, -1 on failure.
 */

extern
int
restore_signals( Signals* save );
/* purpose: restore previously save signals
 * paramtr: save (IN): previously saved signal state
 * returns: 0 on success, count of errors on failure
 * warning: errno may be useless to check in this case
 */ 

extern
void
start_child( char* argv[], char* envp[], Signals* save );
/* purpose: start a child process with stdin connected to /dev/null
 * paramtr: argv (IN): argument vector, NULL terminated
 *          envp (IN): environment vector, NULL terminated
 *          save (IN): if not NULL, saved signals to restore
 * returns: DOES NOT RETURN
 */

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
