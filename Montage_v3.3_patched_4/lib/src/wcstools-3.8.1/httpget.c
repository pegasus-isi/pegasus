/* File httpget.c
 * July 17, 2007
 * By Doug Mink and John Roll

 * Test http access to scat
 * Load with -lnsl -lsocket
 */

#include <stdio.h>
#include <strings.h>

#define CHUNK	8192
#define LINE	1024

FILE *gethttp();
static int all = 0;

main (ac, av)
int ac;
char **av;
{
    FILE *fp;
    int  red;
    char *url;

    if (ac == 1) {
	printf ("HTTPGET: Print contents of URL\n");
	printf ("If -a, all to standard out, else non-data to stderr\n");
	exit (0);
	}

    url = *(av+1);
    if (url[0] == '-' && url[1] == 'a') {
	all = 1;
	url = *(av+2);
	}
    if ( !(fp = gethttp (url)) ) {
	fprintf(stderr, "Can't read URL %s\n", url);
	exit(1);
	}

}


#define NO_XFILE_H

FILE *SokOpen();
#define XFREAD  1
#define XFWRITE 2
#define XFCREAT 4

#define File    FILE *
#define OpenFd(fd, mode)        fdopen(fd, mode)
#define FileFd(fd)              fileno(fd)
#define Malloc(space, size)     ( (space) = (void *) malloc(size) )


File
gethttp (url)
char	*url;
{
    File sok;

    char hosturl[LINE];
    char linebuf[CHUNK];
    char page[LINE];
    char *buffer;
    char *fpage;
    char *port;
    char *cbcont;
    int  nport = 80;
    int  chunked = 0;
    int  lchunk;
    int  status;
    int  red;
    int  diag = 1;
    int  nbcont = 0;
    int  nbr;
    int  i;

    port = NULL;
    strcpy (hosturl, url);
        
    if ( !strncmp(hosturl, "http://", 7) ) {
	strcpy(hosturl, url+7);
	}
    fpage = strchr (hosturl, '/');
    if (fpage != NULL) {
	strcpy (page, fpage);
	*fpage = (char) 0;
	if (port = strchr (hosturl, ':') ) {
	    *port = '\0';
	    port++;
	    nport = atoi (port);
	    }
	}
    else {
	page[0] = '/';
	page[1] = '\0';
	}

    if ( !(sok = SokOpen (hosturl, nport, XFREAD | XFWRITE)) ) {
	if (port != NULL)
	    fprintf(stderr, "Can't read URL %s:%s\n", hosturl, port);
	else
	    fprintf(stderr, "Can't read URL %s\n", hosturl);
	abort();
	}
        
    fprintf(sok, "GET %s HTTP/1.1\nHost: %s\n\n", page, hosturl);
    fflush(sok);

    fscanf(sok, "%*s %d %*s\n", &status);

    if ( status != 200 ) return NULL;

    nbcont = 0;
    while ( fgets(linebuf, LINE, sok) ) {
	if (all)
	    fprintf (stdout, "%s", linebuf);
	else
	    fprintf (stderr, "%s", linebuf);
	if (strsrch (linebuf, "chunked") != NULL)
	    chunked = 1;
	if (strsrch (linebuf, "Content-length") != NULL) {
	    if ((cbcont = strchr (linebuf, ':')) != NULL)
		nbcont = atoi (cbcont+1);
	    }
	if ( *linebuf == '\n' ) break;
	if ( *linebuf == '\r' ) break;
	}

    if (nbcont == 0) {
	fgets (linebuf, LINE, sok);
	if (all)
	    fprintf (stdout, "%s", linebuf);
	else if (diag)
	    fprintf (stderr, "%s", linebuf);
	}

    /* Print result a chunk at a time */
    if (chunked) {
	lchunk = (int) strtol (linebuf, NULL, 16);
	buffer = (char *) calloc (1, CHUNK);
	while (lchunk > 0) {
	    for (i = 0; i < CHUNK; i++)
		buffer[i] = (char) 0;
	    nbr = fread (buffer, 1, lchunk, sok);
	    fprintf (stdout, "%s", buffer);
	    fgets (linebuf, LINE, sok);
	    if (all)
		fprintf (stdout, "%s", linebuf);
	    else if (diag)
		fprintf (stderr, "%s", linebuf);
	    fgets (linebuf, LINE, sok);
	    if (all)
		fprintf (stdout, "%s", linebuf);
	    else if (diag)
		fprintf (stderr, "%s", linebuf);
	    if (strlen (linebuf) < 1)
		break;
	    lchunk = (int) strtol (linebuf, NULL, 16);
	    if (lchunk < 1)
		break;
	    }
	}

    /* Print result all at once */
    else if (nbcont > 0) {
	buffer = (char *) calloc (1, nbcont);
	if ((red = fread (buffer, 1, nbcont, sok)) > 0)
	    fwrite (buffer, 1, red, stdout);
	}

    /* Print result a line at at time */
    else {
	while ( (red = fread (linebuf, 1, CHUNK, sok)) > 0 ) {
	    fwrite (linebuf, 1, red, stdout);
	    }
	}

    return sok;
}


/* sokFile.c
 */
/* copyright 1991, 1993, 1995, 1999 John B. Roll jr.
 */


#ifdef HAVE_FCNTL_H
#include <fcntl.h>
#else
#include <sys/fcntl.h>
#endif

#include <errno.h>

#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/param.h>
#include <netdb.h>

#ifdef BERKLEY
#include <sys/time.h>
#endif

#ifndef NO_XFILE_H
#include "xos.h"
#include "xfile.h"
#endif

#ifdef __STDC__
static int FileINetParse(char *file, int port, struct sockaddr_in *adrinet);
#endif

File SokOpen(name, port, mode)
        char *name;             /* "host:port" socket to open */
        int   port;
        int   mode;             /* mode of socket to open */
{
    int             xfd;        /* socket descriptor */
    int             type;       /* type returned from FileINetParse */
    struct sockaddr_in adrinet; /* socket structure parsed from name */
    int             reuse_addr = 1;


    File            f;          /* returned file descriptor */

    if (!(type = FileINetParse(name, port, &adrinet)))
        return NULL;

    if ( type == 1 
     || (mode & XFCREAT && mode & XFREAD && !(mode & XFWRITE)) ) {
        if ( ((xfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
          ||  setsockopt(xfd, SOL_SOCKET, SO_REUSEADDR,
                     (char *) &reuse_addr, sizeof(reuse_addr)) < 0
          || (bind(xfd, (struct sockaddr *) & adrinet
                         ,sizeof(adrinet)) != 0)
          ||  listen(xfd, 5) ) {
            close(xfd);
            return NULL;
        }
      } else {
        if (((xfd = socket(AF_INET, SOCK_STREAM, 0)) < 0)
                   || (connect(xfd, (struct sockaddr *) & adrinet
                               ,sizeof(adrinet)) != 0)) {
            close(xfd);
            return NULL;
        }
    }

    f = OpenFd(xfd, "r+");

    return f;
}


static int FileINetParse(file, port, adrinet)
        char *file;             /* host/socket pair to parse? */
        int   port;
        struct sockaddr_in *adrinet; /* socket info structure to fill? */
{
    struct hostent *hp;         /* -> hostent structure for host */
    char            hostname[MAXHOSTNAMELEN + 12]; /* name of host */
    char           *portstr;    /* internet port number (ascii) */
    int             type = 2;   /* return code */

    char *strchr();

    if ( !strncmp(file, "http://", 7) ) {
        file += 7;
        if ( port == -1 ) port  = 80;
    }

    strcpy(hostname, file);

#ifdef msdos
    /* This is a DOS disk discriptor, not a machine name */
    if ((!(file[0] == '.')) && file[1] == ':')
        return 0;
#endif

    if ( portstr = strchr(hostname, '/') ) {
        *portstr = '\0';
    }

    if ( portstr = strchr(hostname, ':') ) {
        *portstr++ = '\0';

        if ((port = strtol(portstr, NULL, 0)) == 0) {
            struct servent *getservbyname();
            struct servent *service;

            if ((service = getservbyname(portstr, NULL)) == NULL)
                return 0;
            port = service->s_port;
        }
    }

    if ( port == -1 ) return 0;

    if (hostname[0] == '\0')
        type = 1;
    if (hostname[0] == '\0' || hostname[0] == '.')
        if (gethostname(hostname, MAXHOSTNAMELEN) == -1)
            return 0;

    if ((hp = gethostbyname(hostname)) == NULL)
        return 0;

    memset(adrinet, 0, sizeof(struct sockaddr_in));
    adrinet->sin_family = AF_INET;
    adrinet->sin_port = htons(port);
    memcpy(&adrinet->sin_addr, hp->h_addr, hp->h_length);

    return type;
}

static char *iaddrstr(addr, addrlen)
        struct sockaddr_in *addr; /* socket info structure to fill? */
        int     addrlen;
 {
     struct hostent *hp = NULL;
     char           *name;
 
    if (!(hp = gethostbyaddr((char *) &addr->sin_addr, sizeof(addr->sin_addr)
                        , AF_INET))) {
        unsigned char  *a = (unsigned char *) &addr->sin_addr;

        Malloc(name, 32);
        sprintf(name, "%d.%d.%d.%d:%d", a[0], a[1], a[2], a[3]
                , ntohs(addr->sin_port));
    } else {
        Malloc(name, strlen(hp->h_name) + 10);
        sprintf(name, "%s:%d", hp->h_name, ntohs(addr->sin_port));
    }

    return name;
}

/* Dec  4 2000	New program
 *
 * Mar 27 2001	Fix so colons can be in query part of URL for coordinates
 * Mar 27 2001	Add option to read entire contents at once
 * Jul 12 2001	Make line buffer large enough to read a chunk of data
 */
