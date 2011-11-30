#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <libxml/xmlmemory.h>
#include <libxml/xmlreader.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include "vds-format-kickstart.h"
	
char *tempString;

void initInvo(invocation *newInvo) {
	newInvo->jobs = malloc(sizeof(jtype));
	newInvo->numjobs = 0;
	newInvo->inuname = malloc(6*sizeof(char *));
	newInvo->env = malloc(sizeof(char *));
	newInvo->numenv = 0;
	newInvo->instatcall = malloc(sizeof(statcall));
	newInvo->numcalls = 0;
	newInvo->attr = malloc(15*sizeof(char *));
	newInvo->numattr = 15;
	newInvo->cwd = malloc(sizeof(char));
}

void getInvoProp(invocation *main, xmlNodePtr root) {
	main->attr[0] = (char *)xmlGetProp(root, (xmlChar *)"version");
	main->attr[1] = (char *)xmlGetProp(root, (xmlChar *)"start"); // Start of application according to host clock
	main->attr[2] = (char *)xmlGetProp(root, (xmlChar *)"duration"); //Duration of application run in seconds with microsecond fraction, according to host clock
	main->attr[3] = (char *)xmlGetProp(root, (xmlChar *)"transformation");
	main->attr[4] = (char *)xmlGetProp(root, (xmlChar *)"derivation");
	main->attr[5] = (char *)xmlGetProp(root, (xmlChar *)"resource");
	main->attr[6] = (char *)xmlGetProp(root, (xmlChar *)"hostaddr");
	main->attr[7] = (char *)xmlGetProp(root, (xmlChar *)"hostname");
	main->attr[8] = (char *)xmlGetProp(root, (xmlChar *)"pid");
	main->attr[9] = (char *)xmlGetProp(root, (xmlChar *)"uid");
	main->attr[10] = (char *)xmlGetProp(root, (xmlChar *)"user");
	main->attr[11] = (char *)xmlGetProp(root, (xmlChar *)"gid");
	main->attr[12] = (char *)xmlGetProp(root, (xmlChar *)"group");
	main->attr[13] = (char *)xmlGetProp(root, (xmlChar *)"wfLabel");
	main->attr[14] = (char *)xmlGetProp(root, (xmlChar *)"wfStamp");
}

void setupJob(xmlNodePtr child, invocation *invo) {
	invo->numjobs++;
	invo->jobs = realloc(invo->jobs, invo->numjobs*sizeof(jtype));
	
	// Initialize pointers so that free() will be happy with erasing the memory later
	invo->jobs[invo->numjobs-1].name = malloc(sizeof(char));
	invo->jobs[invo->numjobs-1].start = malloc(sizeof(char));
	invo->jobs[invo->numjobs-1].duration = malloc(sizeof(char));
	invo->jobs[invo->numjobs-1].pid = malloc(sizeof(char));
	invo->jobs[invo->numjobs-1].errortype = malloc(sizeof(char));
	invo->jobs[invo->numjobs-1].corefile = malloc(sizeof(char));
	invo->jobs[invo->numjobs-1].args = malloc(sizeof(char *));
	
	// job name must be one of the following: setup, prejob, mainjob, postjob, cleanup
	invo->jobs[invo->numjobs-1].name = (char *)child->name;
	
	invo->jobs[invo->numjobs-1].start = (char *)xmlGetProp(child, (xmlChar *)"start");
	invo->jobs[invo->numjobs-1].duration = (char *)xmlGetProp(child, (xmlChar *)"duration");
	invo->jobs[invo->numjobs-1].pid = (char *)xmlGetProp(child, (xmlChar *)"pid");
	
	
	for(xmlNodePtr child2 = child->children; child2 != NULL; child2 = child2->next) {
		
		if(child2->type == XML_ELEMENT_NODE) {
			if(xmlStrcmp(child2->name, (xmlChar *)"usage") == 0) {
				invo->jobs[invo->numjobs-1].jusage.utime = atof((char *)xmlGetProp(child2, (xmlChar *)"utime"));
				invo->jobs[invo->numjobs-1].jusage.stime = atof((char *)xmlGetProp(child2, (xmlChar *)"stime"));
				invo->jobs[invo->numjobs-1].jusage.minflt = atoi((char *)xmlGetProp(child2, (xmlChar *)"minflt"));
				invo->jobs[invo->numjobs-1].jusage.majflt = atoi((char *)xmlGetProp(child2, (xmlChar *)"majflt"));
				invo->jobs[invo->numjobs-1].jusage.nswap = atoi((char *)xmlGetProp(child2, (xmlChar *)"nswap"));
				invo->jobs[invo->numjobs-1].jusage.nsignals = atoi((char *)xmlGetProp(child2, (xmlChar *)"nsignals"));
				if((tempString = (char *)xmlGetProp(child2, (xmlChar *)"nvcsw")) != NULL) {
					invo->jobs[invo->numjobs-1].jusage.nvcsw = atoi(tempString);
				}
				if((tempString = (char *)xmlGetProp(child2, (xmlChar *)"nivcsw")) != NULL) {
					invo->jobs[invo->numjobs-1].jusage.nivcsw = atoi(tempString);
				}
			}
			else if(xmlStrcmp(child2->name, (xmlChar *)"status") == 0) {
				
				invo->jobs[invo->numjobs-1].rawstatus = atoi((char *)xmlGetProp(child2, (xmlChar *)"raw"));
				
				xmlNodePtr child3 = child2->children;
				
				while(child3->type != XML_ELEMENT_NODE) { child3 = child3->next; }
				if(xmlStrcmp(child3->name, (xmlChar *)"regular") == 0) {
					invo->jobs[invo->numjobs-1].errortype = (char *)child3->name;
					invo->jobs[invo->numjobs-1].exitcode = atoi((char *)xmlGetProp(child3, (xmlChar *)"exitcode"));
				}
				else if(xmlStrcmp(child3->name, (xmlChar *)"failure") == 0) {
					invo->jobs[invo->numjobs-1].errortype = (char *)child3->name;
					invo->jobs[invo->numjobs-1].exitcode = atoi((char *)xmlGetProp(child3, (xmlChar *)"error"));
				}
				else if(xmlStrcmp(child3->name, (xmlChar *)"signalled") == 0) {
					invo->jobs[invo->numjobs-1].errortype = (char *)child3->name;
					invo->jobs[invo->numjobs-1].exitcode = atoi((char *)xmlGetProp(child3, (xmlChar *)"signal"));
					invo->jobs[invo->numjobs-1].corefile = (char *)xmlGetProp(child3, (xmlChar *)"corefile");
				}
				else if(xmlStrcmp(child3->name, (xmlChar *)"suspended") == 0) {
					invo->jobs[invo->numjobs-1].errortype = (char *)child3->name;
					invo->jobs[invo->numjobs-1].exitcode = atoi((char *)xmlGetProp(child3, (xmlChar *)"suspended"));
				}
			}
			
			else if(xmlStrcmp(child2->name, (xmlChar *)"statcall") == 0) {
				setupStatCall(child2, &invo->jobs[invo->numjobs-1].jstatcall);
			}
			
			else if(xmlStrcmp(child2->name, (xmlChar *)"argument-vector") == 0) {
				
				for(xmlNodePtr child5 = child2->children; child5 != NULL; child5 = child5->next) {
					
					if(child5->type == XML_ELEMENT_NODE) {
						if(xmlStrcmp(child5->name, (xmlChar *)"arg") == 0) {
								strcpy(tempString, (char *)xmlGetProp(child5, (xmlChar *)"nr"));
								invo->jobs[invo->numjobs-1].numargs = atoi(tempString);
								invo->jobs[invo->numjobs-1].args = realloc(invo->jobs[invo->numjobs-1].args, invo->jobs[invo->numjobs-1].numargs*sizeof(char *));
								invo->jobs[invo->numjobs-1].args[invo->jobs[invo->numjobs-1].numargs-1] = malloc(1024*sizeof(char));
								strcpy(invo->jobs[invo->numjobs-1].args[invo->jobs[invo->numjobs-1].numargs-1], (char *)xmlNodeGetContent(child5));
						}
					}
					
				}
			}
			else if(xmlStrcmp(child2->name, (xmlChar *)"arguments")==0) {
				invo->jobs[invo->numjobs-1].numargs = 1;
                                invo->jobs[invo->numjobs-1].args = realloc(invo->jobs[invo->numjobs-1].args, invo->jobs[invo->numjobs-1].numargs*sizeof(char *));
				invo->jobs[invo->numjobs-1].args[invo->jobs[invo->numjobs-1].numargs-1] = malloc(1024*sizeof(char));
				strcpy(invo->jobs[invo->numjobs-1].args[invo->jobs[invo->numjobs-1].numargs-1], (char *)xmlNodeGetContent(child2));
			}
		}
	}
}

void setupStatCall(xmlNodePtr child, statcall *setup) {

	// More malloc to appease the free() gods.
	setup->type = malloc(sizeof(char));
	setup->filename = malloc(sizeof(char));
	setup->statinfo = malloc(12*sizeof(char *));
	setup->data = malloc(sizeof(char));
	setup->ident = malloc(sizeof(char));

	xmlNodePtr child2 = child->children;
	
	setup->error = atoi((char *)xmlGetProp(child, (xmlChar *)"error"));
	setup->ident = (char *)xmlGetProp(child, (xmlChar *)"id");
	
	for(child2 = child2->next; child2 != NULL; child2 = child2->next) {
	
		if(child2->type == XML_ELEMENT_NODE) {
			
			if(xmlStrcmp(child2->name, (xmlChar *)"file") == 0) {
				setup->type = strdup("file");
				setup->filename = (char *)xmlGetProp(child2, (xmlChar *)"name");
			}
			else if(xmlStrcmp(child2->name, (xmlChar *)"descriptor") == 0) {
				setup->type = strdup("descriptor");
				setup->descriptor = atoi((char *)xmlGetProp(child2, (xmlChar *)"number"));
			}
			else if(xmlStrcmp(child2->name, (xmlChar *)"temporary") == 0) {
				setup->type = strdup("temporary");
				setup->filename = (char *)xmlGetProp(child2, (xmlChar *)"name");
				setup->descriptor = atoi((char *)xmlGetProp(child2, (xmlChar *)"descriptor"));
			}
			else if(xmlStrcmp(child2->name, (xmlChar *)"fifo") == 0) {
				setup->type = strdup("fifo");
				setup->filename = (char *)xmlGetProp(child2, (xmlChar *)"name");
				setup->descriptor = atoi((char *)xmlGetProp(child2, (xmlChar *)"descriptor"));
				if((tempString = (char *)xmlGetProp(child2, (xmlChar *)"count")) != NULL ) {
					setup->count = atoi(tempString);
				}
				if((tempString = (char *)xmlGetProp(child2, (xmlChar *)"rsize")) != NULL ) {
					setup->rsize = atoi(tempString);
				}
				if((tempString = (char *)xmlGetProp(child2, (xmlChar *)"wsize")) != NULL ) {
					setup->wsize = atoi(tempString);
				}
			}
			
			else if(xmlStrcmp(child2->name, (xmlChar *)"statinfo") == 0) {
				setup->statinfo[0] = (char *)xmlGetProp(child2, (xmlChar *)"size");
				setup->statinfo[1] = (char *)xmlGetProp(child2, (xmlChar *)"mode");
				setup->statinfo[2] = (char *)xmlGetProp(child2, (xmlChar *)"inode");
				setup->statinfo[3] = (char *)xmlGetProp(child2, (xmlChar *)"nlink");
				setup->statinfo[4] = (char *)xmlGetProp(child2, (xmlChar *)"blksize");
				setup->statinfo[5] = (char *)xmlGetProp(child2, (xmlChar *)"atime");
				setup->statinfo[6] = (char *)xmlGetProp(child2, (xmlChar *)"mtime");
				setup->statinfo[7] = (char *)xmlGetProp(child2, (xmlChar *)"ctime");
				setup->statinfo[8] = (char *)xmlGetProp(child2, (xmlChar *)"uid");
				setup->statinfo[9] = (char *)xmlGetProp(child2, (xmlChar *)"user");
				setup->statinfo[10] = (char *)xmlGetProp(child2, (xmlChar *)"gid");
				setup->statinfo[11] = (char *)xmlGetProp(child2, (xmlChar *)"group");
			}
			else if(xmlStrcmp(child2->name, (xmlChar *)"data") == 0) {
				setup->data = (char *)xmlNodeGetContent(child2);
			}
		}
	}
}

void Free(void *ptr) {
	if(ptr != NULL) {
		free(ptr);
	}
}

void destroyInvo(invocation *minvo) {
	int i;
	for(i=0;i<minvo->numjobs;i++) {
		Free(minvo->jobs[i].start);
		Free(minvo->jobs[i].duration);
		Free(minvo->jobs[i].pid);
		Free(minvo->jobs[i].corefile);
		Free(minvo->jobs[i].args);
		Free(minvo->jobs[i].jstatcall.type);
		Free(minvo->jobs[i].jstatcall.filename);
		Free(minvo->jobs[i].jstatcall.statinfo);
		Free(minvo->jobs[i].jstatcall.data);
		Free(minvo->jobs[i].jstatcall.ident);
	}
	Free(minvo->jobs);
	Free(minvo->cwd);
	Free(minvo->inuname);
	Free(minvo->env);
	Free(minvo->attr);
	for(i=0;i<minvo->numcalls;i++) {
		Free(minvo->instatcall[i].type);
		Free(minvo->instatcall[i].filename);
		Free(minvo->instatcall[i].statinfo);
		Free(minvo->instatcall[i].data);
		Free(minvo->instatcall[i].ident);
	}
	Free(minvo->instatcall);
}

void getInfo(xmlNodePtr kickRoot, invocation *mainInvo) {

	if(xmlStrcmp(kickRoot->name, (xmlChar *)"invocation") != 0) {
		fprintf(stderr, "Incorrect style of xml\n");
		exit(E_BADXML);
	}
	else {
		getInvoProp(mainInvo, kickRoot);
	}
	
	for(xmlNodePtr child = kickRoot->children; child != NULL; child = child->next) {
		if(child->type == XML_ELEMENT_NODE) {
			if(xmlStrcmp(child->name, (xmlChar *)"setup") == 0 || //setup, prejob, mainjob, postjob, cleanup
				xmlStrcmp(child->name, (xmlChar *)"prejob") == 0 ||
				xmlStrcmp(child->name, (xmlChar *)"mainjob") == 0 ||
				xmlStrcmp(child->name, (xmlChar *)"postjob") == 0 ||
				xmlStrcmp(child->name, (xmlChar *)"cleanup") == 0) {
				setupJob(child, mainInvo);
			}
			else if(xmlStrcmp(child->name, (xmlChar *)"cwd") == 0) {
				mainInvo->cwd = (char *)xmlNodeGetContent(child);
			}
			else if(xmlStrcmp(child->name, (xmlChar *)"usage") == 0) {
				mainInvo->inuse.utime = atof((char *)xmlGetProp(child, (xmlChar *)"utime"));
				mainInvo->inuse.stime = atof((char *)xmlGetProp(child, (xmlChar *)"stime"));
				mainInvo->inuse.minflt = atoi((char *)xmlGetProp(child, (xmlChar *)"minflt"));
				mainInvo->inuse.majflt = atoi((char *)xmlGetProp(child, (xmlChar *)"majflt"));
				mainInvo->inuse.nswap = atoi((char *)xmlGetProp(child, (xmlChar *)"nswap"));
				mainInvo->inuse.nsignals = atoi((char *)xmlGetProp(child, (xmlChar *)"nsignals"));
				if((tempString = (char *)xmlGetProp(child, (xmlChar *)"nvcsw")) != NULL) {
					mainInvo->inuse.nvcsw = atoi(tempString);
				}
				if((tempString = (char *)xmlGetProp(child, (xmlChar *)"nivcsw")) != NULL) {
					mainInvo->inuse.nivcsw = atoi(tempString);
				}
			}
			else if(xmlStrcmp(child->name, (xmlChar *)"uname") == 0) {
				mainInvo->inuname[0] = (char *)xmlGetProp(child, (xmlChar *)"archmode");
				mainInvo->inuname[1] = (char *)xmlGetProp(child, (xmlChar *)"system");
				mainInvo->inuname[2] = (char *)xmlGetProp(child, (xmlChar *)"nodename");
				mainInvo->inuname[3] = (char *)xmlGetProp(child, (xmlChar *)"release");
				mainInvo->inuname[4] = (char *)xmlGetProp(child, (xmlChar *)"machine");
				mainInvo->inuname[5] = (char *)xmlGetProp(child, (xmlChar *)"domainname");
			}
			else if(xmlStrcmp(child->name, (xmlChar *)"environment") == 0) {
				for(xmlNodePtr child2 = child->children; child2 != NULL; child2 = child2->next) {
					if(child2->type == XML_ELEMENT_NODE) {
						mainInvo->numenv += 2;
						mainInvo->env = realloc(mainInvo->env, mainInvo->numenv*sizeof(char *));
						mainInvo->env[mainInvo->numenv-2] = (char *)xmlGetProp(child2, (xmlChar *)"key");
						mainInvo->env[mainInvo->numenv-1] = (char *)xmlNodeGetContent(child2);
					}
				}
			}
			else if(xmlStrcmp(child->name, (xmlChar *)"statcall") == 0) {
				mainInvo->numcalls++;
				mainInvo->instatcall = realloc(mainInvo->instatcall, mainInvo->numcalls*sizeof(statcall));
				setupStatCall(child, &mainInvo->instatcall[mainInvo->numcalls-1]);
			}
		}
		
	}
}

//								  2006-07-24 T16:30:12. 696-05:00
// mainInvo->start in the form of YYYY-MM-DD Thh:mm:ss. ms -hh:mm
//								  0123456789 0123456789 012345678

// returns string in format: Jun 23, 2006 12:34

char *parseDate(char *date) {
	char *tempString1, *tempString2;
	char *monthstr, *parsemonth, *parsedate, *parseyear;
	char *time;
	char *finstring;
	int tempInt1, tempInt2, month;
	short timeint;
	
	time = malloc(25*sizeof(char));
	
	parsemonth = malloc(2*sizeof(char));
	finstring = malloc(256*sizeof(char));
	monthstr = malloc(5*sizeof(char));
	
	parsemonth[0] = date[5];
	parsemonth[1] = date[6];
	
	month = atoi(parsemonth);

	switch(month) {
		case 1:
			strcpy(monthstr, "Jan ");
			break;
		case 2:
			strcpy(monthstr, "Feb ");
			break;
		case 3:
			strcpy(monthstr, "Mar ");
			break;
		case 4:
			strcpy(monthstr, "Apr ");
			break;
		case 5:
			strcpy(monthstr, "May ");
			break;
		case 6:
			strcpy(monthstr, "Jun ");
			break;
		case 7:
			strcpy(monthstr, "Jul ");
			break;
		case 8:
			strcpy(monthstr, "Aug ");
			break;
		case 9:
			strcpy(monthstr, "Sep ");
			break;
		case 10:
			strcpy(monthstr, "Oct ");
			break;
		case 11:
			strcpy(monthstr, "Nov ");
			break;
		case 12:
			strcpy(monthstr, "Dec ");
			break;
		default:
			fprintf(stderr, "Error parsing date");
			exit(E_SYSERR);
	}

	strcpy(finstring, monthstr);
	parsedate = malloc(3*sizeof(char));
	
	//parsedate[0] = date[8];
	//parsedate[1] = date[9];
	parsedate[2] = ' ';
	
	//sprintf(parsedate, "%c%c%c", date[8], date[9], ' ');
	strncpy(parsedate, &date[8], 2);

	finstring = strncat(finstring, parsedate, 3);

	parseyear = malloc(5*sizeof(char));
	
	//parseyear[0] = date[0];
	//parseyear[1] = date[1];
	//parseyear[2] = date[2];
	//parseyear[3] = date[3];

	//sprintf(parseyear, "%c%c%c%c%c", date[0], date[1], date[2], date[3], ' ');
	strncpy(parseyear, date, 4);
	parseyear[4] = ' ';
	
	finstring = strncat(finstring, parseyear, 5);
					

	tempString1 = malloc(2*sizeof(char));
	tempString2 = malloc(3*sizeof(char));
	
	//tempString1[0] = date[11];
	//tempString1[1] = date[12];
	
	//sprintf(tempString1,"%c%c", date[11], date[12]);
	
	strncpy(tempString1, &date[11], 2);

	//tempString2[0] = date[23];
	//tempString2[1] = date[24];
	//tempString2[2] = date[25];
	
	//sprintf(tempString2,"%c%c%c", date[23], date[24], date[25]);
	strncpy(tempString2, &date[23], 3);
	
	tempInt1 = atoi(tempString1);
	tempInt2 = atoi(tempString2);
	
	timeint = tempInt1 + tempInt2;
	
	sprintf(time, "%d", timeint);
	
	finstring = strcat(finstring, time);
	finstring = strcat(finstring, ":");

	//tempString1[0] = date[15];
	//tempString1[1] = date[14];
	
	//sprintf(tempString1,"%c%c", date[14], date[15]);
	free(tempString1);
	tempString1 = malloc(20*sizeof(char));
	strncpy(tempString1, &date[14], 2);
	
	free(tempString2);
	tempString2 = malloc(20*sizeof(char));	
	tempString2[0] = date[23];
	//tempString2[1] = date[27];
	//tempString2[2] = date[28];
	
	//sprintf(tempString2,"%c%c%c", date[23], date[27], date[28]);
	strncat(tempString2, &date[27], 2);
	
	tempInt1 = atoi(tempString1);
	tempInt2 = atoi(tempString2);
	
	timeint = tempInt1 + tempInt2;
	
	if(timeint > 9) {
		sprintf(time, "%d", timeint);
	}
	else {
		sprintf(time, "0%d", timeint);		
	}

	finstring = strcat(finstring, time);
	
	free(tempString1);
	free(tempString2);
	free(parsemonth);
	free(parseyear);	
	free(parsedate);
	free(monthstr);
	free(time);
	
	return finstring;
}

char *getFileNameOnly(char *path) {
	int i;
	i = strlen(path);
	while(i>0 && path[i] != '/') {
		i--;
	}
	if(i==0) { return path; }
	return &path[i+1];
}


