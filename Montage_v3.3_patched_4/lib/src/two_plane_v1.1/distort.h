#ifndef _distort_h_
#define _distort_h_
#define MAXORDER 10

/* struct for coefficient */
typedef struct {
  int    A_ORDER;                /* max power for the 1st dimension    */
  double A[MAXORDER][MAXORDER];  /* coefficient array of 1st dimension */
  int    B_ORDER;                /* max power for 1st dimension        */
  double B[MAXORDER][MAXORDER];  /* coefficient array of 2nd dimension */
  int    AP_ORDER;               /* max power for the 1st dimension    */
  double AP[MAXORDER][MAXORDER]; /* coefficient array of 1st dimension */
  int    BP_ORDER;               /* max power for 1st dimension        */
  double BP[MAXORDER][MAXORDER]; /* coefficient array of 2nd dimension */
  double crpix1;
  double crpix2;
  double a_dmax;
  double b_dmax;
} DistCoeff;

#endif
