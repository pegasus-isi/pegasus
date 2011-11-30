#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <libxml/xmlmemory.h>
#include <libxml/xmlreader.h>
#include <libxml/parser.h>
#include <libxml/tree.h>

#define SUCCESS (0)
#define E_BADPARAMS (1)
#define E_BADXML (2)
#define E_SYSERR (3)
#define MALLOC_CHECK_ (1)

struct usage {

		double utime;            // Total amount of user time used, in seconds with millisecond fraction.
		double stime;			// Total amount of system time used, in seconds with millisecond fraction
		int minflt;				// Number of soft page faults
		int majflt;				// Number of hard page faults
		int nswap;				// Number of times a process was swapped out of physical memory
		int nsignals;			// Number of signals delivered
		int nvcsw;				// Number of voluntary context switches
		int nivcsw;				// Number of involuntary context switches
};

typedef struct usage usage;
 

// UNAME string array conform to the following:

/*
		0 -- archmode			IA32, IA64, ILP32, LP64
		1 -- system	
		2 -- nodename
		3 -- release
		4 -- machine
		5 -- domainname
 */



/*		STATINFO array in the statcall struct contains the following information
 *		0 -- size
 *		1 -- mode
 *		2 -- inode
 *		3 -- nlink
 *		4 -- blksize
 *		5 -- atime
 *		6 -- mtime
 *		7 -- ctime
 *		8 -- uid
 *		9 -- user
 *		10 - gid
 *		11 - group
 */

struct statcall {
	char *ident;
	int error;			//Result from the stat call on a named file or descriptor.
	char *filename;
	char *type;
	int descriptor;
	int count;
	int rsize;
	int wsize;
	char **statinfo;
	char *data;
};

typedef struct statcall statcall;

struct jobType {
	char *name;  // One of the following: setup, prejob, mainjob, postjob, cleanup
	char *start;
	char *duration;
	char *pid;
	struct usage jusage;
	int rawstatus;
	char *errortype;
	int exitcode;
	char *corefile;
	struct statcall jstatcall;
	int numargs;
	char **args;
};

typedef struct jobType jtype;

/*	Attr string array contains the following attributes of the invocation type:
 *     0 -- version
 *     1 -- start
 *     2 -- duration				Duration of application run in seconds with microsecond fractions
 *     3 -- transformation
 *     4 -- derivation
 *     5 -- resource
 *     6 -- hostaddr
 *     7 -- hostname
 *     8 -- pid
 *     9 -- uid
 *     10 - user
 *     11 - gid
 *     12 - group
 *     13 - wfLabel
 *     14 - wfStamp
 */

struct invocation {
	int numjobs;
	jtype *jobs;
	char *cwd;
	struct usage inuse;
	char **inuname; //uname string array
	char **env; //environment variables
	int numenv;
	statcall *instatcall;
	int numcalls;
	char **attr;
	int numattr;
};

typedef struct invocation invocation;

void initInvo(invocation *newInvo);
void getInvoProp(invocation *main, xmlNodePtr root);
void setupJob(xmlNodePtr child, invocation *invo);
void setupStatCall(xmlNodePtr child, statcall *setup);
void destroyInvo(invocation *minvo);
void getInfo(xmlNodePtr kickRoot, invocation *mainInvo);
void printkrec(invocation *minvo);
char *parseDate(char *date);
char *getFileNameOnly(char *path);



