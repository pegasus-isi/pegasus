#include "distort.h"

distort(double x, double y, DistCoeff coeff, double *u, double *v)
{
  int m, n, i, j, k;
  double s[MAXORDER], sum;
  double temp_x, temp_y;

  m = coeff.AP_ORDER;
  n = coeff.BP_ORDER;

  temp_x = x - coeff.crpix1;
  temp_y = y - coeff.crpix2;

  /* compute u */
  for (j=0; j<=m; j++) {
    s[j] = coeff.AP[m-j][j];
    for (k=j-1; k>=0; k--) {
      s[j] = temp_y*s[j] + coeff.AP[m-j][k];
    }
  }
  
  sum = s[0];
  for (i=m; i>=1; i--){
    sum = temp_x*sum + s[m-i+1];
  }
  *u = sum;

  /* compute v*/
  for (j=0; j<=n; j++) {
    s[j] = coeff.BP[n-j][j];
    for (k=j-1; k>=0; k--) {
      s[j] = temp_y*s[j] + coeff.BP[n-j][k];
    }
  }
   
  sum = s[0];
  for (i=n; i>=1; i--)
    sum = temp_x*sum + s[n-i+1];

  *v = sum;

  *u = x + *u;
  *v = y + *v;
}
