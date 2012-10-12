#ifndef _redefine_pointing_h_
#define _redefine_pointing_h_
const char *RefinedCRVAL1 = "RARFND";
const char *RefinedCRVAL2 = "DECRFND";
const char *RefinedCROTA2 = "CT2RFND";
const char *CRVAL1 = "CRVAL1";
const char *CRVAL2 = "CRVAL2";
const char *CROTA2 = "CROTA2";
const char *RA_MOVING = "RA_REF";
const char *DEC_MOVING = "DEC_REF"; 

const char *CD11 = "CD1_1";
const char *CD12 = "CD1_2";
const char *CD21 = "CD2_1";
const char *CD22 = "CD2_2";

const char *RefinedCD11 = "CD11RFND";
const char *RefinedCD12 = "CD12RFND";
const char *RefinedCD21 = "CD21RFND";
const char *RefinedCD22 = "CD22RFND";

/*if unable to read the refined values return value
has the bit corresponding to the failed keywords set
if unable to rewrite the refined values return value
has the bit corresponding to the failed keywords set
times (-1)
*/
int redefine_pointing(char *fitsheader, int verbose);
int parse_double(char *fitsheader, double *value, const char *key);
int parse_int(char *fitsheader, int *value, const char *key);
int replace_keyword(char *fitsheader, double value, const char *key);
int moving_object_pointing_replacement(char *fitsheader, double restRA, double restDec,
							  int verbose);
#endif
