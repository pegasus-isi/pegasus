/* File httpscat.c
 * November 2000
 * By John Roll

 * Test http access to scat
 * Load with -lnsl -lsocket
 */

#include <stdio.h>
#include <strings.h>

#define CHUNK   8192
#define LINE    1024

FILE *scathttp();

main() {
        FILE    *fp;
        int      red;
        char     cat[CHUNK];

        if ( !(fp = scathttp("http://cfa-www/catalog/scat"
                        , "ua2", 10.0, 10.0, "J2000", 360.0, 20.0, "")) ) {
                fprintf(stderr, "Can't scat catalog\n");
                exit(1);
        }

        while ( (red = fread(cat, 1, CHUNK, fp)) > 0 ) {
                fwrite(cat, 1, red, stdout);
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


File scathttp(url, catalog, ra, dec, system, radius, mag, sort)
        char    *url;
        char    *catalog;
        double   ra;
        double   dec;
        char    *system;
        double   radius;
        double   mag;
        char    *sort;
{
        File     sok;

        char     URL[LINE];
        char     HST[LINE];
        char     BUF[LINE];
        char    *page;

        int      status;

        strcpy(HST, url);
        
        if ( !strncmp(HST, "http://", 7) ) {
            strcpy(HST, url+7);
        }
        if ( page = strchr(HST, ':') ) {
            *page = '\0';
        }
        if ( page = strchr(HST, '/') ) {
            *page = '\0';
             page++;
        }

        sprintf(URL, "/%s?catalog=%s&ra=%f&dec=%f&system=%s&radius=%f&&mag=%f&sort=%s"
                , page, catalog, ra, dec, system, radius, mag, sort);

        if ( !(sok = SokOpen(url, 80, XFREAD | XFWRITE)) ) {
                abort();
        }
        
        fprintf(sok, "GET %s HTTP/1.1\nHost: %s\n\n", URL, HST);
        fflush(sok);

        fscanf(sok, "%*s %d %*s\n", &status);

        if ( status != 200 ) return NULL;

        while ( fgets(BUF, LINE, sok) ) {
                if ( *BUF == '\n' ) break;
                if ( *BUF == '\r' ) break;
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
