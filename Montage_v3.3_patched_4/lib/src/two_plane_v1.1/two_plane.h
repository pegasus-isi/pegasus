/*  "By David Makovoz (davidm\@ipac.caltech.edu)\n".
  "Copyright (C) 2004 California Institute of Technology";
*/
#ifndef _two_plane_h_
#define _two_plane_h_

#include "distort.h"
#include "wcs.h"
#define CANNOT_PROJECT 2
#define PROJECT_OUTSIDE_IMAGE 1
#define TWO_PLANE_NOT_INITIALIZED -1
#ifdef PI180
#undef PI180
#endif
#define	PI180                   0.0174532925199433
#define Epsilon                 0.00000000000000000000001
struct TwoPlane{
  char projection_type_1[4];
  char projection_type_2[4];
  double x_center_1;
  double y_center_1;
  double x_center_2;
  double y_center_2;
  double cos_phi_1;
  double sin_phi_1;
  double cos_phi_2;
  double sin_phi_2;
  double cos_theta;
  double sin_theta;
  double cdelt1_1;
  double cdelt2_1;
  double cdelt1_2;
  double cdelt2_2;
  int naxis1_1;
  int naxis1_2;
  int naxis2_1;
  int naxis2_2;
  DistCoeff DistortCoeffFirst; 
  DistCoeff DistortCoeffSecond;
  int first_distorted;
  int second_distorted;
  int initialized;
  int have_cdmatrix1;
  double cd1_11;
  double cd1_12;
  double cd1_21;
  double cd1_22;
  double invcd1_11;
  double invcd1_12;
  double invcd1_21;
  double invcd1_22;
  int have_cdmatrix2;
  double cd2_11;
  double cd2_12;
  double cd2_21;
  double cd2_22;
  double invcd2_11;
  double invcd2_12;
  double invcd2_21;
  double invcd2_22;
};

int plane1_to_plane2_transform(double x_in, double y_in, double *x_out, double *y_out, 
	      struct TwoPlane *two_plane);
int plane2_to_plane1_transform(double x_in, double y_in, double *x_out, double *y_out, 
		      struct TwoPlane *two_plane);
int Initialize_TwoPlane(struct TwoPlane *two_plane, struct WorldCoor *wcs, struct WorldCoor *WCS);

int Initialize_TwoPlane_FirstDistort(struct TwoPlane *two_plane, 
				     char *fitsheader, struct WorldCoor *WCS); 
int Initialize_TwoPlane_SecondDistort(struct TwoPlane *two_plane,  
				      struct WorldCoor *wcs, char *fitsheader); 
int Initialize_TwoPlane_BothDistort(struct TwoPlane *two_plane,  
 				     char *fitsheader1, char *fitsheader2); 

int SetDistortionPlaneFirst(struct TwoPlane *two_plane, 
			  int a_order, double *a,
                          int b_order, double *b,
                          int ap_order, double *ap, 
                          int bp_order, double *bp,
			    double, double);
int SetDistortionPlaneSecond(struct TwoPlane *two_plane, 
			  int a_order, double *a,
                          int b_order, double *b,
                          int ap_order, double *ap, 
                          int bp_order, double *bp,
			    double, double);
int SetDistortionPlaneFirstSimple(struct TwoPlane *two_plane, 
			    DistCoeff *coeff);

int SetDistortionPlaneSecondSimple(struct TwoPlane *two_plane, 
			     DistCoeff *coeff);
#endif
