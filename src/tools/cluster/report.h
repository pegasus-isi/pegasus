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

#ifndef _REPORT_H
#define _REPORT_H

#include <sys/types.h>
#include <time.h>
#include <sys/resource.h>

extern
int
find_application( char* argv[] );
/* purpose: find start of argv excluding kickstart
 * paramtr: argv (IN): invocation argument vector
 * returns: start of argv. Returns 0 if unsure.
 */

extern
ssize_t
report( int progress, double start, double duration,
	int status, char* argv[], struct rusage* use, 
	const char* special );
/* purpose: report what has just finished.
 * paramtr: progress (IN): file description open for writing
 *          start (IN): start time (no millisecond resolution)
 *          duration (IN): duration with millisecond resolution
 *          status (IN): return value from wait() family 
 *          argv (IN): NULL-delimited argument vector of app
 *          use (IN): resource usage from wait4() call
 *          special (IN): set for setup/cleanup jobs.
 * returns: number of bytes written onto "progress"
 */

#endif /* _REPORT_H */
