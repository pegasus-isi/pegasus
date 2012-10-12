#include "distort.h"
undistort(double u, double v, DistCoeff coeff, double *x, double *y)
{
  int m, n, i, j, k;
  double s[MAXORDER], sum;
  double temp_u, temp_v;
  m = coeff.A_ORDER;
  n = coeff.B_ORDER;

  temp_u = u - coeff.crpix1;
  temp_v = v - coeff.crpix2;
  /* compute u */
  for (j=0; j<=m; j++) {
    s[j] = coeff.A[m-j][j];
    for (k=j-1; k>=0; k--) {
      s[j] = temp_v*s[j] + coeff.A[m-j][k];
    }
  }
  
  sum = s[0];
  for (i=m; i>=1; i--){
    sum = temp_u*sum + s[m-i+1];
  }
  *x = sum;

  /* compute v*/
  for (j=0; j<=n; j++) {
    s[j] = coeff.B[n-j][j];
    for (k=j-1; k>=0; k--) {
      s[j] =temp_v*s[j] + coeff.B[n-j][k];
    }
  }
   
  sum = s[0];
  for (i=n; i>=1; i--)
    sum = temp_u*sum + s[n-i+1];

  *y = sum;

  *x = u + *x;
  *y = v + *y;
/*   *x = u + *x + coeff.crpix1; */
/*   *y = v + *y + coeff.crpix2; */
}
