#define NED_NAME_RESOLVER      1
#define NED_BYNAME             2
#define NED_NEARNAME           3
#define NED_NEARPOSN           4
#define NED_IAUFORMAT          5
#define NED_XREFCODE           6  /* expand_refcode */
#define NED_REF                7  /* ref_objname */

#define NED_MALLOC(dtype, n)   (dtype *)malloc(n*sizeof(dtype))
#define NED_MCHK(p)            if (!p) { ned_errno=NE_NOSPACE; return(-1);}


