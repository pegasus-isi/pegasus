#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>
#include <getopt.h>
#include <time.h>
#include <libxml/xmlmemory.h>
#include <libxml/xmlreader.h>
#include <libxml/parser.h>
#include <libxml/tree.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include "vds-format-kickstart.h"

void pgusage(char *progname)
{
	fprintf(stderr, "Usage: %s [--getdata | -v] [--help | -h] kickstart.xml\n", progname);
	exit(E_BADPARAMS);
}

int main(int argc, char **argv) {
	char kickfile[strlen(argv[argc - 1])], *kickfilename, opt;
	kickfilename=malloc(strlen(argv[argc-1])*sizeof(char));
	int i, j;
	int getdata = 0;
	int option_index=0;
	invocation mainInvo;
	
	struct option long_options[] = {
		{"getdata", 0, 0, 'v'},
		{"help", 0, 0, 'h'},
		{0, 0, 0, 0}
	};
	
	if(argc == 1) {
		pgusage(strdup(argv[0]));
	}
	
	while ((opt = getopt_long (argc, argv, "vh:", long_options, &option_index)) != -1)
	{
		switch(opt)
		{
			case 'v':
				getdata=1;
				break;
			case 'h':
				pgusage(argv[0]);
			case '?':
				pgusage(argv[0]);
			default:
				fprintf(stderr, "ERROR: Unrecognized option\n");
				pgusage(argv[0]);
		}
	}
	strcpy(kickfile, argv[argc-1]);
	
	strcpy(kickfilename, getFileNameOnly(kickfile));

	//setbuf(stdout, 0);

	int temp, toread;
	char buffer[8096];
	if((temp = open(kickfile, O_RDONLY)) == -1) {
		printf("%s\t", kickfilename);
		fflush(NULL);
		fprintf(stderr, "Failed to open file\n");
		return E_SYSERR;
	}
	else {
		if((toread = read(temp, buffer, 8000)) < 1) {
			printf("%s\t", kickfilename);
			fflush(NULL);
			fprintf(stderr, "Empty or corrupt file\n"); 
			return E_SYSERR;
		}
		else {
			close(temp);
		}
	}
	
	xmlDocPtr kickDoc;
	xmlNodePtr kickRoot;
	
	xmlParserCtxtPtr ctxtkick;
	ctxtkick = xmlNewParserCtxt();
	if (ctxtkick == NULL) {
		printf("%s\t", kickfilename);
		fflush(NULL);
        	fprintf(stderr, "Failed to allocate parser context\n");
		return E_SYSERR;
	}
    
	kickDoc = xmlCtxtReadFile(ctxtkick, kickfile, NULL, XML_PARSE_NOERROR | XML_PARSE_RECOVER);
	if((kickRoot = xmlDocGetRootElement(kickDoc)) == NULL) {
		printf("%s\t", kickfilename);
		fflush(NULL);
		fprintf(stderr, "Failed to parse xml\n");
		return E_BADXML;
	}
	
	initInvo(&mainInvo);
	
	getInfo(kickRoot, &mainInvo);

	if(!getdata) {	
		printf("%s\t%s\t%s\t",mainInvo.attr[10], parseDate(mainInvo.attr[1]), mainInvo.attr[2]);
		for(i = 0; i<mainInvo.numcalls; i++) {
			if(strcmp(mainInvo.instatcall[i].ident,"stdout")==0 || strcmp(mainInvo.instatcall[i].ident,"stderr")==0) {
				if(mainInvo.instatcall[i].type != NULL && strcmp(mainInvo.instatcall[i].type,"temporary")==0) {
					printf("TEMP\t");
				}
				else {
					printf("NTEMP\t");
				}
			}
		}

		if(mainInvo.numjobs == 1) {
				if(mainInvo.jobs[0].rawstatus >= 0) {
					if(strcmp(mainInvo.jobs[0].errortype, "regular") == 0) {
						printf("r%d\t", mainInvo.jobs[0].exitcode);
					}
					else if(strcmp(mainInvo.jobs[0].errortype, "failure") == 0) {
						printf("f%d\t", mainInvo.jobs[0].exitcode);
					}
					else if(strcmp(mainInvo.jobs[0].errortype, "signalled") == 0) {
						printf("s%d\t", mainInvo.jobs[0].exitcode);
					}
					else if(strcmp(mainInvo.jobs[0].errortype, "failure") == 0) {
						printf("sus\t");
					}
				}
				else { printf("rf%d\t", mainInvo.jobs[0].rawstatus); }
				int k = strlen(kickfilename);
				kickfilename = realloc(kickfilename, 40*sizeof(char));
                                while(k < 40) { kickfilename[k] = ' '; k++; }
				printf("%s", kickfilename);
				printf("%s ", mainInvo.jobs[0].jstatcall.filename);
				for(i=0; i<mainInvo.jobs[0].numargs; i++) { printf("%s ", mainInvo.jobs[0].args[i]); }
		}
		else {
			for(i=0;i<mainInvo.numjobs;i++) {
				if(mainInvo.jobs[i].rawstatus >= 0) {
					if(strcmp(mainInvo.jobs[i].errortype, "regular") == 0) {
						printf("r%d\t", mainInvo.jobs[i].exitcode);
					}
					else if(strcmp(mainInvo.jobs[i].errortype, "failure") == 0) {
						printf("f%d\t", mainInvo.jobs[i].exitcode);
					}
					else if(strcmp(mainInvo.jobs[i].errortype, "signalled") == 0) {
						printf("s%d\t", mainInvo.jobs[i].exitcode);
					}
					else if(strcmp(mainInvo.jobs[i].errortype, "suspended") == 0) {
						printf("sus\t");
					}
				}
				else { printf("%d\t", mainInvo.jobs[i].rawstatus); }
				int k = strlen(kickfilename);
                                kickfilename = realloc(kickfilename, 40*sizeof(char));
                                while(k < 40) { kickfilename[k] = ' '; k++; }
				printf("%s", kickfilename);
				printf("%s: %s ", mainInvo.jobs[i].name, mainInvo.jobs[i].jstatcall.filename);
				for(j=i;j<mainInvo.jobs[i].numargs;j++) { printf("%s ", mainInvo.jobs[i].args[j]); }
			}
		}
	}
	else {
		printf("%s\t", kickfilename);
		printf("\n");
		for(i=0;i<mainInvo.numcalls;i++) {
			if(strcmp(mainInvo.instatcall[i].ident,"stdout")==0) {
				printf("************************\n");
				printf("*\tSTDOUT\n");
				printf("************************\n");
				if(strcmp(mainInvo.instatcall[i].type,"temporary")==0) {
					printf("%s\n",mainInvo.instatcall[i].data);
				}
				else {
					printf("Redirected to: %s\n", mainInvo.instatcall[i].filename);
				}
			}
			else if(strcmp(mainInvo.instatcall[i].ident,"stderr")==0) {
				printf("************************\n");
				printf("*\tSTDERR\n");
				printf("************************\n");if(strcmp(mainInvo.instatcall[i].type,"temporary")==0) {
					printf("%s\n",mainInvo.instatcall[i].data);
				}
				else {
					printf("Redirected to: %s\n", mainInvo.instatcall[i].filename);
				}
			}
		}
		printf("\n\n");
	}
	
	
	printf("\n");
	destroyInvo(&mainInvo);
	xmlFreeNode(kickRoot);
	xmlUnlinkNode(kickRoot);
	xmlFreeDoc(kickDoc);
	
	return SUCCESS;
	
}

