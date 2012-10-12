/* Module: mArchiveList.c


Version	Developer		Date	Change
-------	---------------	-------	-----------------------
1.0		John Good		14Dec04	Baseline code
2.0		Rajiv Mayani	06Mar12	Two level request

*/

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <math.h>
#include <strings.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>

#define URL	0
#define GPFS 	1
#define GFTP 	2
#define URI	3

#define TRUE	1
#define FALSE	0

#define MAXLEN 20000

char *url_encode (char *s);

int tcp_connect(char *hostname, int port);
int readline	(int fd, char *line) ;
int write_file (int socket, const char* filename);
int parseUrl(char *urlStr, char *hostStr, int *port);
int get_socket (char *server, int port);

void initialize ();
void set_server (char *survey, char *server);
void set_base (char *survey, char *base);
void set_constraint (char *survey, char *constraint, char *type, char **parameters);
void current (char *type, char **parameters);
void two_level (char *type, char **parameters);
void level_one (char **parameters, char **url);

FILE* create_file (const char* filename);

int debug = 0;

char *proxy;
char pserver [MAXLEN];

int	pport;

/************************************************/
/*						*/
/* mArchiveList -- Given a location on the	*/
/* sky, archive name, and size in degrees	*/
/* contact the IRSA server to retreive		*/
/* a list of archive images.	The list	*/
/* contains enough information to support	*/
/* mArchiveGet downloads.			*/
/*						*/
/************************************************/

int main(int argc, char **argv)
{
	char	source	[MAXLEN];
	char	type	[MAXLEN];

	/* Construct service request using location/size */

	strcpy(type, "url");

	if(argc > 2 && strcmp(argv[1], "-s") == 0)
	{
		strcpy(source, argv[2]);

		argc -= 2;
		argv += 2;

		if(strncasecmp(source, "gf", 2) == 0
		|| strncasecmp(source, "gr", 2) == 0)
	 strcpy(type, "gftp");

		if(strncasecmp(source, "gp", 2) == 0)
	 strcpy(type, "gpfs");

		if(strncasecmp(source, "nvo", 3) == 0
		|| strncasecmp(source, "uri", 3) == 0)
	 strcpy(type, "uri");
	}

	if(argc < 7)
	{
		printf("[struct stat=\"ERROR\", msg=\"Usage: %s survey band object|location width height outfile (object/location must be a single argument string)\"]\n", argv[0]);
		exit(0);
	}

	initialize ();

	if (strncmp (argv[1], "2mass", 5) == 0)
	{
		// Calling one-request service for 2mass.
		current (type, argv);
	}
	else
	{
		// Calling two-request service for other datasets.
		two_level (type, argv);
	}
	
	fflush(stdout);
	exit(0);
}

/********************************************************************************************/
/* Returns the part of the string, where the content starts. (left-trim)                    */
/********************************************************************************************/

char* data_start_index (char* band)
{
	if (band == NULL)
		return NULL;

	int i, length = strlen (band);	
	if (length == 0)
		return NULL;

	for (i = 0; i < length; ++i)
	{
		if (band [i] != ' ')
		{
			return band + i;
		}
	}
}

/********************************************************************************************/
/* Parse response from second call of the two-level service.        	                    */
/* Checks if content has band column; If yes, filter data based on band passed in arguments */
/********************************************************************************************/

void parse_level_two (int socket, const char* band, const char* filename)
{
	char line [MAXLEN];
	char *tokens;
	char *temp;

	int count = 0;
	int filter = FALSE, header_found = FALSE;
	int filter_loc_start, filter_loc_end;

	FILE* fout = create_file (filename);

	while(1)
	{
		/* Read lines returning from service */
		if(readline (socket, line) == 0)
			break;

		if(debug)
		{
			printf("DEBUG> return; [%s]\n", line);
			fflush(stdout);
		}

		if(strncmp(line, "ERROR: ", 7) == 0)
		{
			if(line[strlen(line)-1] == '\n')
				line[strlen(line)-1]	= '\0';

			printf("[struct stat=\"ERROR\", msg=\"%s\"]\n", line+7);
			exit(0);
		}
		else
		{
			if(header_found == FALSE && line[0] == '|')
			{
 				/* Check if header row has a "band" column */
				header_found = TRUE;
				fprintf(fout, "%s", line);
				
				tokens = strtok (line, "|");
		 		while (tokens != NULL)
		 		{
					if ((temp = strstr (tokens, "band")) != NULL)
					{
						filter = TRUE;
						filter_loc_start = tokens - line;
						filter_loc_end = filter_loc_start + strlen (tokens);
					}
					tokens = strtok (NULL, "|");
		 		}
			 }
			else
			{
				if (line[0] == '\\' || line[0] == '|')
				{
					// Write to file.
					fprintf (fout, "%s", line);
				}
				else
				{	
					if (header_found == FALSE) // Do not print HTTP Headers
						continue;

					/* If content has band column, compare column value with, band passed inarguments */
					if (filter == TRUE)
					{
						temp = strndup (line + filter_loc_start, filter_loc_end - filter_loc_start);
						temp = data_start_index (temp);
						if (temp == NULL)
						{
							printf("[struct stat=\"ERROR\", msg=\"Expected to find content in column band %s\"]\n", temp);
							exit(0);
						}
					}
					
					if (filter == FALSE || strcmp (data_start_index (temp), band) == 0)
					{
						++count;
						fprintf(fout, "%s", line);
					}
				}
			}
		}
	}
	
	printf("[struct stat=\"OK\", count=\"%d\"]\n", count);

	fflush (fout);
	fclose (fout);
}

/************************************************/
/* Second call of the two-level service.       	*/
/************************************************/

void level_two (const char *url, const char* band, const char* filename)
{
	int socket, port = 80, count;

	char	server	[MAXLEN];
	char	base	[MAXLEN];
	char	constraint[MAXLEN];
	char	request	[MAXLEN];

	/* Connect to the port on the host we want */
	socket = get_socket (server, port);

	/* Send a request for the file we want */
	if(!proxy) 
	{
		sprintf(request, "GET %s HTTP/1.0\r\n\r\n",
		 url);
	} 
	else 
	{
		sprintf(request, "GET %s HTTP/1.0\r\nHOST: %s:%d\r\n\r\n",
		 base, server, port);
	}

	if(debug)
	{
		printf("DEBUG> request = [%s]\n", request);
		fflush(stdout);
	}

	send(socket, request, strlen(request), 0);

	parse_level_two (socket, band, filename);
}

/************************************************************************/
/* Main method of two level service                                    	*/
/************************************************************************/

void two_level (char *type, char **parameters)
{
	char urll [MAXLEN];
	char *url = &urll[0];

	level_one (parameters, &url);

	if(debug)
	{
		printf("DEBUG> url [%s]\n", url);
		fflush(stdout);
	}

	level_two (url, parameters [2], parameters [6]);
}

/************************************************************************/
/* Set request parameters for first call of two level service         	*/
/************************************************************************/

void level_one_constraint (char *location, char *height_str, char *width_str, char *constraint)
{
	float height = atof (height_str);
	float width = atof (width_str);

	if (height <= 0 || width <= 0)
	{
		printf("[struct stat=\"ERROR\", msg=\"Height and Width cannot be <= 0\"]\n");
		exit(0);
	}

	float radius = sqrt ( (height * height) + (width * width));

	sprintf(constraint, "location=%s&radius=%f", url_encode(location), radius);

	if(debug)
	{
		printf("DEBUG> constraint [%s]\n", constraint);
		fflush(stdout);
	}
}

/********************************************************************/
/* Parse response from first call of the two-level service.        	*/
/* Extract URL for second call                                   	*/
/********************************************************************/

void parse_level_one (int socket, const char* dataset, char **url)
{
	char line [MAXLEN];
	char *tokens;
	char *temp;	

	int identifier_loc_start, identifier_loc_end;
	int display_loc_start, display_loc_end;

	while(1)
	{
		/* Read lines returning from service */
		if(readline (socket, line) == 0)
			break;

		if(debug)
		{
			printf("DEBUG> return; [%s]\n", line);
			fflush(stdout);
		}

		if(strncmp(line, "ERROR: ", 7) == 0)
		{
			if(line[strlen(line)-1] == '\n')
				line[strlen(line)-1]	= '\0';

			printf("[struct stat=\"ERROR\", msg=\"%s\"]\n", line+7);
			exit(0);
		}
		else
		{
			if(line[0] == '|')
			{
				tokens = strtok (line, "|");
		 		while (tokens != NULL)
		 		{
					// Extract location of identifier, and display columns in the content.
					if ((temp = strstr (tokens, "identifier")) != NULL)
					{
						identifier_loc_start = tokens - line;
						identifier_loc_end = identifier_loc_start + strlen (tokens);

					}
					else if ((temp = strstr (tokens, "display")) != NULL)
					{
						display_loc_start = tokens - line;
						display_loc_end = display_loc_start + strlen (tokens);
					}
					tokens = strtok (NULL, "|");
		 		 }
			 }
			else if (line[0] != '\\' && line[0] != '|')
			{
				// Extract URL for requested data-set.
				if ((temp = strstr (line, dataset)) != NULL)
				{
					if (temp - line >= identifier_loc_start && temp - line <= identifier_loc_end)
					{
						*url = strndup (line + display_loc_start, display_loc_end - display_loc_start);
						return;
					}
				}
			}
		}
	}

	printf("[struct stat=\"ERROR\", msg=Dataset \"%s\" not found.]\n", dataset);
	exit(0);
}

/************************************************/
/* First call of the two-level service.        	*/
/************************************************/

void level_one (char **parameters, char **url)
{
	int 	socket, port = 80, count;

	char	server		[MAXLEN];
	char	base		[MAXLEN];
	char	constraint	[MAXLEN];
	char	request		[MAXLEN];

	set_server (parameters[1], server);
	set_base (parameters[1], base);

	level_one_constraint (parameters [3], parameters [5], parameters [4], constraint);

	/* Connect to the port on the host we want */
	socket = get_socket (server, port);

	/* Send a request for the file we want */
	if(proxy) 
	{
		sprintf(request, "GET http://%s:%d%s%s HTTP/1.0\r\n\r\n",
		 server, port, base, constraint);
	} 
	else 
	{
		sprintf(request, "GET %s%s HTTP/1.0\r\nHOST: %s:%d\r\n\r\n",
		 base, constraint, server, port);
	}
	
	if(debug)
	{
		printf("DEBUG> request = [%s]\n", request);
		fflush(stdout);
	}

	send(socket, request, strlen(request), 0);

	parse_level_one (socket, parameters[1], url);
}

/********************************/
/* Single call service.        	*/
/********************************/

void current (char *type, char **parameters)
{
	int socket, port = 80, count;

	char	server	[MAXLEN];
	char	base		[MAXLEN];
	char	constraint[MAXLEN];
	char	request	[MAXLEN];

	set_server		(parameters[1], server);
	set_base   		(parameters[1], base);
	set_constraint	(parameters[1], constraint, type, parameters);

	/* Connect to the port on the host we want */
	socket = get_socket (server, port);

	/* Send a request for the file we want */
	if(proxy) 
	{
		sprintf(request, "GET http://%s:%d%s%s HTTP/1.0\r\n\r\n",
		 server, port, base, constraint);
	} 
	else 
	{
		sprintf(request, "GET %s%s HTTP/1.0\r\nHOST: %s:%d\r\n\r\n",
		 base, constraint, server, port);
	}

	if(debug)
	{
		printf("DEBUG> request = [%s]\n", request);
		fflush(stdout);
	}

	send(socket, request, strlen(request), 0);

	/* And read all the lines coming back */
	count = write_file (socket, parameters[6]);
	printf("[struct stat=\"OK\", count=\"%d\"]\n", count);
}

/********************************/
/* Set server location         	*/
/********************************/

void set_server (char *survey, char *server)
{
	if (strncmp (survey, "2mass", 5) == 0)
	{
		strcpy(server, "irsa.ipac.caltech.edu");
		return;
	}

	strcpy(server, "vao-web.ipac.caltech.edu");
}

/********************************/
/* Set base URL location       	*/
/********************************/

void set_base (char *survey, char *base)
{
	if (strncmp (survey, "2mass", 5) == 0)
	{
		strcpy(base, "/cgi-bin/ImgList/nph-imglist?");
		return;
	}

	strcpy(base, "/cgi-bin/ImgSearch/nph-imgSearch?");
}

/****************************************************/
/* Set request parameters for old service         	*/
/****************************************************/

void set_constraint (char *survey, char *constraint, char *type, char **parameters)
{
	sprintf(constraint, "survey=%s&band=%s&location=%s&width=%s&height=%s&mode=%s",
		url_encode(parameters[1]), url_encode(parameters[2]), 
		url_encode(parameters[3]), url_encode(parameters[4]), 
		url_encode(parameters[5]), type);
}

/****************************************************************************/
/* Check if proxy is being used, if yes set proxy server and port         	*/
/****************************************************************************/

void initialize ()
{
	/* Connect to the port on the host we want */
	proxy = getenv("http_proxy");
	
	if(proxy) 
	{
		parseUrl(proxy, pserver, &pport);
		if(debug)
		{
			printf("DEBUG> proxy = [%s]\n", proxy);
			printf("DEBUG> pserver = [%s]\n", pserver);
			printf("DEBUG> pport = [%d]\n", pport);
			fflush(stdout);
		}
	}
}

/****************************************************************************/
/* Create socket, based on if proxy is being used                        	*/
/****************************************************************************/

int get_socket (char *server, int port)
{
	if(proxy) 
	{
		return tcp_connect(pserver, pport);
	} 
	else 
	{
		return tcp_connect(server, port);
	}
}

/****************************************************************************/
/* Reads lines from descriptor and write to file if no error were found.	*/
/****************************************************************************/

int write_file (int socket, const char* filename)
{
	FILE* fout = create_file (filename);
	char line [MAXLEN];
	int count = 0;
    int start_write = FALSE;

	while(1)
	{
		/* Read lines returning from service */
		if(readline (socket, line) == 0)
		 break;

		if(debug)
		{
			printf("DEBUG> return; [%s]\n", line);
			fflush(stdout);
		}

		if(strncmp(line, "ERROR: ", 7) == 0)
		{
			if(line[strlen(line)-1] == '\n')
				line[strlen(line)-1]	= '\0';

			printf("[struct stat=\"ERROR\", msg=\"%s\"]\n", line+7);
			exit(0);
		}
		else
		{
			if(line[0] == '|'
			|| line[0] == '\\')
				start_write = TRUE;

			if (start_write)
			{
				fprintf(fout, "%s", line);
				fflush(fout);
			}

			if(line[0] != '|'
			&& line[0] != '\\')
				++count;
		}
	}

	fclose(fout);

	return count;
}

/********************************************************/
/* Creates a file and checks for error in file creation	*/
/********************************************************/

FILE* create_file (const char *filename)
{
	FILE* fout = fopen(filename, "w+");

	if(fout == (FILE *)NULL)
	{
		printf("[struct stat=\"ERROR\", msg=\"Can't open output file %s\"]\n",
		 filename);
		exit(0);
	}

	return fout;
}

/***********************************************/
/* This is the basic "make a connection" stuff */
/***********************************************/

int tcp_connect(char *hostname, int port)
{
	int				 sock_fd;
	struct hostent	 *host;
	struct sockaddr_in	sin;


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
/* from a raw file descriptor			*/
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
		break ;	/* unexpected EOF */
		}
		else 
	 return -1 ;
	}

	*line = 0 ;
	return n ;
}

/**************************************/
/* This routine URL-encodes a string	*/
/**************************************/

static unsigned char hexchars[] = "0123456789ABCDEF";

char *url_encode(char *s)
{
	int		len;
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
				(str[j] < 'A' && str[j] > '9')					||
				(str[j] > 'Z' && str[j] < 'a' && str[j] != '_')	||
				(str[j] > 'z'))
		{
		 str[j++] = '%';

		 str[j++] = hexchars[(unsigned char) s[i] >> 4];

		 str[j]	= hexchars[(unsigned char) s[i] & 15];
		}

		++j;
	}

	str[j] = '\0';

	return ((char *) str);
}


int parseUrl(char *urlStr, char *hostStr, int *port) {
	
	char	*hostPtr;
	char	*portPtr;
	char	*dataref;
	char	save;

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
