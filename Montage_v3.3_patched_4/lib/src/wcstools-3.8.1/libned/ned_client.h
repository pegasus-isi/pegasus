/***************************************************************************
/*  ned_client.h      Xiuqin Wu  Nov. 9, 1993
/*
/***************************************************************************/

/* definition of error codes NE_  */
#define NE_NAME             1  /* obj name not recognized by NED name
				  interpreter EGRET */ 
#define NE_AMBN             2  /* ambiguous input name */
#define NE_RA               3  /* RA is  out [0.0, 360.0] */
#define NE_DEC              4  /* DEC is out [-90.0, 90.0] */
#define NE_RADIUS           5  /* radius is out (0.0, 300] */
#define NE_JB               6  /* equinox starts with J or B */
#define NE_EPOCH            7  /*  epoch is out [1500.0, 2500.0] */
#define NE_IAU              8  /* unacceptible IAU format  */
#define NE_NOBJ             9  /* no object found */
#define NE_NOSPACE          10  /* memory allocation failure */
#define NE_QUERY            11 /* can't send query to server, 
				  connection problem */
#define NE_HOST             12  /* can't get hostent from the name */
#define NE_SERVICE          13  /* can't get the port number for the service */
#define NE_PROTO            14  /* can't conver tcp to protocol number */
#define NE_SOCK             15  /* can't get a socket allocated */
#define NE_CONNECT          16  /* can't connect to server */
#define NE_BROKENC          17  /* the connection is broken */
#define NE_TBLSIZE          18  /* can't get table size */
#define NE_EREFC            19  /* the refcode is not a 19-digit code */
#define NE_NOREFC           20  /* no detailed infomation for the refcode */
#define NE_NOREF            21  /* no reference for given objname */

typedef struct _crossid {
   char  objname[31];
   char  objtype[7];
   } CrossID;


typedef struct _more_data {
   char       data_typec[61];   /* data type code to describe the data */
   char       data[101];
   struct _more_data *next;
   } MoreData;


typedef struct _obj_info {
   int        no_crossid;  /* number of crossids for the object */
   CrossID   *cp;
   double     dist;        /* distance to the search center */
   int        no_ref;      /* number of references */
   int        no_note;     /* number of notes */
   int        no_photom;   /* number of photometric data points */
   char       objtype[7];
   double     ra;          /* J2000 */
   double     dec;
   double     unc_maj;
   double     unc_min;
   double     unc_ang;
   char       refcode[20];
   MoreData   *mdp;
   } ObjInfo;


typedef struct _ned_ref {
   char  refcode[20];
   char  pub_name[81];
   int   year;
   char  vol[21];
   char  page[21];
   char  title1[81];
   char  title2[81];
   char  author1[81];
   char  author2[81];
   } NedRef;

/* Subroutines */
int connect_server();
int disconnect_server();
int ned_connect();
void ned_disconnect();
int ned_ex_refcode();
void ned_free_cp();
void ned_free_objp();
void ned_free_refp();
int ned_get_one_ref();
int ned_getdouble();
char *ned_get_errmsg();
int ned_getint();
int ned_gets();
int ned_name_resolver();
int ned_obj_byname();
int ned_obj_iau();
int ned_obj_nearname();
int ned_obj_nearpos();
int ned_obj_nearposn();
int ned_objects();
int ned_objname_search();
int ned_query();
int ned_qst();
int ned_qno();
int ned_ref();
int radius_out_range();
int send_cmd();
int clean_obj_socket();
int connectSock();
int disconnectSock();
int send_cmdSock();
int get_stringSock();
int read_inbuf();
