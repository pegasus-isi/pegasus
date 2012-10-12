/* Module: mHdr.c


Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.1      John Good        21Jul06  Baseline code
1.0      John Good        21Jul06  Baseline code

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <string.h>
#include <errno.h>


#define MAXLEN 20000

extern char *optarg;
extern int optind, opterr;

extern int getopt (int argc, char *const *argv, const char *options);

char  *url_encode (char *s);
int    tcp_connect(char *hostname, int port);
int    readline   (int fd, char *line);

int parseUrl(char *urlStr, char *hostStr, int *port);

int debug = 0;


/********************************************/
/*                                          */
/* mHdr -- Create a header template file    */
/* from location, size, resolution and      */
/* rotation inputs.                         */
/*                                          */
/********************************************/

int main(int argc, char **argv)
{
   int    ch, sock, port, count;
  
   char   line      [MAXLEN];
   char   request   [MAXLEN];
   char   base      [MAXLEN];
   char   constraint[MAXLEN];
   char   server    [MAXLEN];
   char   outfile   [MAXLEN];
   char   bandStr   [MAXLEN];
   char   band2MASS [MAXLEN];

   char  *locstr;
   char  *widthstr;

   char   heightstr [MAXLEN];
   char   sysstr    [MAXLEN];
   char   equistr   [MAXLEN];
   char   resstr    [MAXLEN];
   char   rotstr    [MAXLEN];

   FILE  *fout;

   char  *proxy;
   char   pserver   [MAXLEN];
   int    pport;

   /* Construct service request using location/size */

   opterr = 0;

   strcpy(heightstr, "");
   strcpy(sysstr,    "");
   strcpy(equistr,   "");
   strcpy(resstr,    "");
   strcpy(rotstr,    "");

   strcpy(band2MASS, "");
   
   while ((ch = getopt(argc, argv, "s:e:h:p:r:t:")) != EOF)
   {
      switch (ch)
      {
         case 's':
            strcpy(sysstr, optarg);
            break;

         case 'e':
            strcpy(equistr, optarg);
            break;

         case 'h':
            strcpy(heightstr, optarg);
            break;

         case 'p':
            strcpy(resstr, optarg);
            break;

         case 'r':
            strcpy(rotstr, optarg);
            break;

         case 't':
	    strcpy(bandStr, optarg);

	         if(bandStr[0] == 'j') strcpy(band2MASS, "j");
	    else if(bandStr[0] == 'h') strcpy(band2MASS, "h");
	    else if(bandStr[0] == 'k') strcpy(band2MASS, "k");
	    else if(bandStr[0] == 'J') strcpy(band2MASS, "j");
	    else if(bandStr[0] == 'H') strcpy(band2MASS, "h");
	    else if(bandStr[0] == 'K') strcpy(band2MASS, "k");

	    else
	    {
	       printf("[struct stat=\"ERROR\", msg=\"If 2MASS band is given, it must be 'J', 'H', or 'K'\"]\n");
	       exit(0);
	    }

            break;

         default:
            break;
      }
   }

   if(argc < 4)
   {
      printf("[struct stat=\"ERROR\", msg=\"Usage: %s [-s system] [-e equinox] [-h height(deg)] [-p pixsize(arcsec)] [-r rotation] [-t 2mass-band] object|location width(deg) outfile (object/location must be a single argument string)\"]\n", argv[0]);
      exit(0);
   }

   strcpy(server, "irsa.ipac.caltech.edu");

   port = 80;

   strcpy(base, "/cgi-bin/HdrTemplate/nph-hdr?");

   locstr    = url_encode(argv[optind]);
   widthstr  = url_encode(argv[optind+1]);

   strcpy(outfile, argv[optind+2]);

   sprintf(constraint, "location=%s&width=%s",
      locstr, widthstr);

   if(strlen(heightstr) > 0)
   {
      strcat(constraint, "&height=");
      strcat(constraint, url_encode(heightstr));
   }

   if(strlen(sysstr) > 0)
   {
      strcat(constraint, "&system=");
      strcat(constraint, url_encode(sysstr));
   }

   if(strlen(equistr) > 0)
   {
      strcat(constraint, "&equinox=");
      strcat(constraint, url_encode(equistr));
   }

   if(strlen(resstr) > 0)
   {
      strcat(constraint, "&resolution=");
      strcat(constraint, url_encode(resstr));
   }

   if(strlen(rotstr) > 0)
   {
      strcat(constraint, "&rotation=");
      strcat(constraint, url_encode(rotstr));
   }

   if(strlen(band2MASS) > 0)
   {
      strcat(constraint, "&band=");
      strcat(constraint, band2MASS);
   }

   fout = fopen(outfile, "w+");

   if(fout == (FILE *)NULL)
   {
      printf("[struct stat=\"ERROR\", msg=\"Can't open output file %s\"]\n", 
	 outfile);
      exit(0);
   }


   /* Connect to the port on the host we want */

   proxy = getenv("http_proxy");
   
   if(proxy) {
     parseUrl(proxy, pserver, &pport);
     if(debug)
       {
	 printf("DEBUG> proxy = [%s]\n", proxy);
	 printf("DEBUG> pserver = [%s]\n", pserver);
	 printf("DEBUG> pport = [%d]\n", pport);
	 fflush(stdout);
       }
     sock = tcp_connect(pserver, pport);
   } else {
     sock = tcp_connect(server, port);
   }
  

   /* Send a request for the file we want */

   if(proxy) {
     sprintf(request, "GET http://%s:%d%s%s HTTP/1.0\r\n\r\n",
	     server, port, base, constraint);
   } else {
     sprintf(request, "GET %s%s HTTP/1.0\r\nHOST: %s:%d\r\n\r\n",
	     base, constraint, server, port);
   }

   if(debug)
   {
      printf("DEBUG> request = [%s]\n", request);
      fflush(stdout);
   }

   send(sock, request, strlen(request), 0);


   /* And read all the lines coming back */

   count = 0;

   while(1)
   {
      /* Read lines returning from service */

      if(readline (sock, line) == 0)
	 break;

      if(debug)
      {
	 printf("DEBUG> return; [%s]\n", line);
	 fflush(stdout);
      }

      if(strncmp(line, "ERROR: ", 7) == 0)
      {
	 if(line[strlen(line)-1] == '\n')
	    line[strlen(line)-1]  = '\0';

	 printf("[struct stat=\"ERROR\", msg=\"%s\"]\n", line+7);
	 exit(0);
      }
      else
      {
	 fprintf(fout, "%s", line);
	 fflush(fout);

	 if(line[0] != '|'
	 && line[0] != '\\')
	    ++count;
      }
   }
      
   fclose(fout);

   printf("[struct stat=\"OK\", count=\"%d\"]\n", count);
   fflush(stdout);

   exit(0);
}




/***********************************************/
/* This is the basic "make a connection" stuff */
/***********************************************/

int tcp_connect(char *hostname, int port)
{
   int                 sock_fd;
   struct hostent     *host;
   struct sockaddr_in  sin;


   if((host = gethostbyname(hostname)) == NULL) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Couldn't find host %s\"]\n", hostname);
      fflush(stdout);
      return(0);
   }

   if((sock_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) 
   {
      printf("[struct stat=\"ERROR\", msg=\"Couldn't create socket()\"]\n");
      fflush(stdout);
      return(0);
   }

   sin.sin_family = AF_INET;
   sin.sin_port = htons(port);
   bcopy(host->h_addr, &sin.sin_addr, host->h_length);

   if(connect(sock_fd, (struct sockaddr *)&sin, sizeof(sin)) < 0)
   {
      printf("[struct stat=\"ERROR\", msg=\"%s: connect failed.\"]\n", hostname);
      fflush(stdout);
      return(0);
   }

   return sock_fd;
}




/***************************************/
/* This routine reads a line at a time */
/* from a raw file descriptor          */
/***************************************/

int readline (int fd, char *line) 
{
   int n, rc = 0;
   char c ;

   for (n = 1 ; n < MAXLEN ; n++)
   {
      if ((rc == read (fd, &c, 1)) != 1)
      {
	 *line++ = c ;
	 if (c == '\n')
	    break ;
      }

      else if (rc == 0)
      {
	 if (n == 1)
	    return 0 ; /* EOF */
	 else
	    break ;    /* unexpected EOF */
      }
      else 
	 return -1 ;
   }

   *line = 0 ;
   return n ;
}




/**************************************/
/* This routine URL-encodes a string  */
/**************************************/

static unsigned char hexchars[] = "0123456789ABCDEF";

char *url_encode(char *s)
{
   int      len;
   register int i, j;
   unsigned char *str;

   len = strlen(s);

   str = (unsigned char *) malloc(3 * strlen(s) + 1);

   j = 0;

   for (i=0; i<len; ++i)
   {
      str[j] = (unsigned char) s[i];

      if (str[j] == ' ')
      {
         str[j] = '+';
      }
      else if ((str[j] < '0' && str[j] != '-' && str[j] != '.') ||
               (str[j] < 'A' && str[j] > '9')                   ||
               (str[j] > 'Z' && str[j] < 'a' && str[j] != '_')  ||
               (str[j] > 'z'))
      {
         str[j++] = '%';

         str[j++] = hexchars[(unsigned char) s[i] >> 4];

         str[j]   = hexchars[(unsigned char) s[i] & 15];
      }

      ++j;
   }

   str[j] = '\0';

   return ((char *) str);
}


int parseUrl(char *urlStr, char *hostStr, int *port) {
  
   char  *hostPtr;
   char  *portPtr;
   char  *dataref;
   char   save;

   if(strncmp(urlStr, "http://", 7) != 0)
   {
      printf("[struct stat=\"ERROR\", msg=\"Invalid URL string (must start 'http://')\n"); 
      exit(0);
   }

   hostPtr = urlStr + 7;

   dataref = hostPtr;

   while(1)
   {
      if(*dataref == ':' || *dataref == '/' || *dataref == '\0')
	 break;
      
      ++dataref;
   }

   save = *dataref;

   *dataref = '\0';

   strcpy(hostStr, hostPtr);

   *dataref = save;


   if(*dataref == ':')
   {
      portPtr = dataref+1;

      dataref = portPtr;

      while(1)
      {
	 if(*dataref == '/' || *dataref == '\0')
	    break;
	 
	 ++dataref;
      } 

      *dataref = '\0';

      *port = atoi(portPtr);

      *dataref = '/';

      if(*port <= 0)
      {
	 printf("[struct stat=\"ERROR\", msg=\"Illegal port number in URL\"]\n");
	 exit(0);
      }
   }
}
