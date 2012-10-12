/*  Nutation, IAU 2000b model */
/*  Translated from Pat Wallace's Fortran subroutine iau_nut00b (June 26 2007)
    into C by Doug Mink on September 5, 2008 */

#define NLS	77 /* number of terms in the luni-solar nutation model */

void
compnut (dj, dpsi, deps, eps0)

double	dj;	/* Julian Date */
double *dpsi;   /* Nutation in longitude in radians (returned) */
double *deps;   /* Nutation in obliquity in radians (returned) */
double *eps0;   /* Mean obliquity in radians (returned) */

/*  This routine is translated from the International Astronomical Union's
 *  SOFA (Standards Of Fundamental Astronomy) software collection.
 *
 *  notes:
 *
 *  1) the nutation components in longitude and obliquity are in radians
 *     and with respect to the equinox and ecliptic of date.  the
 *     obliquity at j2000 is assumed to be the lieske et al. (1977) value
 *     of 84381.448 arcsec.  (the errors that result from using this
 *     routine with the iau 2006 value of 84381.406 arcsec can be
 *     neglected.)
 *
 *     the nutation model consists only of luni-solar terms, but includes
 *     also a fixed offset which compensates for certain long-period
 *     planetary terms (note 7).
 *
 *  2) this routine is an implementation of the iau 2000b abridged
 *     nutation model formally adopted by the iau general assembly in
 *     2000.  the routine computes the mhb_2000_short luni-solar nutation
 *     series (luzum 2001), but without the associated corrections for
 *     the precession rate adjustments and the offset between the gcrs
 *     and j2000 mean poles.
 *
 *  3) the full IAU 2000a (mhb2000) nutation model contains nearly 1400
 *     terms.  the IAU 2000b model (mccarthy & luzum 2003) contains only
 *     77 terms, plus additional simplifications, yet still delivers
 *     results of 1 mas accuracy at present epochs.  this combination of
 *     accuracy and size makes the IAU 2000b abridged nutation model
 *     suitable for most practical applications.
 *
 *     the routine delivers a pole accurate to 1 mas from 1900 to 2100
 *     (usually better than 1 mas, very occasionally just outside 1 mas).
 *     the full IAU 2000a model, which is implemented in the routine
 *     iau_nut00a (q.v.), delivers considerably greater accuracy at
 *     current epochs;  however, to realize this improved accuracy,
 *     corrections for the essentially unpredictable free-core-nutation
 *     (fcn) must also be included.
 *
 *  4) the present routine provides classical nutation.  the
 *     mhb_2000_short algorithm, from which it is adapted, deals also
 *     with (i) the offsets between the gcrs and mean poles and (ii) the
 *     adjustments in longitude and obliquity due to the changed
 *     precession rates.  these additional functions, namely frame bias
 *     and precession adjustments, are supported by the sofa routines
 *     iau_bi00 and iau_pr00.
 *
 *  6) the mhb_2000_short algorithm also provides "total" nutations,
 *     comprising the arithmetic sum of the frame bias, precession
 *     adjustments, and nutation (luni-solar + planetary).  these total
 *     nutations can be used in combination with an existing IAU 1976
 *     precession implementation, such as iau_pmat76, to deliver gcrs-to-
 *     true predictions of mas accuracy at current epochs.  however, for
 *     symmetry with the iau_nut00a routine (q.v. for the reasons), the
 *     sofa routines do not generate the "total nutations" directly.
 *     should they be required, they could of course easily be generated
 *     by calling iau_bi00, iau_pr00 and the present routine and adding
 *     the results.
 *
 *  7) the IAU 2000b model includes "planetary bias" terms that are fixed
 *     in size but compensate for long-period nutations.  the amplitudes
 *     quoted in mccarthy & luzum (2003), namely dpsi = -1.5835 mas and
 *     depsilon = +1.6339 mas, are optimized for the "total nutations"
 *     method described in note 6.  the luzum (2001) values used in this
 *     sofa implementation, namely -0.135 mas and +0.388 mas, are
 *     optimized for the "rigorous" method, where frame bias, precession
 *     and nutation are applied separately and in that order.  during the
 *     interval 1995-2050, the sofa implementation delivers a maximum
 *     error of 1.001 mas (not including fcn).
 *
 *  References from original Fortran subroutines:
 *
 *     Hilton, J. et al., 2006, Celest.Mech.Dyn.Astron. 94, 351
 *
 *     Lieske, J.H., Lederle, T., Fricke, W., Morando, B., "Expressions
 *     for the precession quantities based upon the IAU 1976 system of
 *     astronomical constants", Astron.Astrophys. 58, 1-2, 1-16. (1977)
 *
 *     Luzum, B., private communication, 2001 (Fortran code
 *     mhb_2000_short)
 *
 *     McCarthy, D.D. & Luzum, B.J., "An abridged model of the
 *     precession-nutation of the celestial pole", Cel.Mech.Dyn.Astron.
 *     85, 37-49 (2003)
 *
 *     Simon, J.-L., Bretagnon, P., Chapront, J., Chapront-Touze, M.,
 *     Francou, G., Laskar, J., Astron.Astrophys. 282, 663-683 (1994)
 *
 */

{ 
    double as2r = 0.000004848136811095359935899141; /* arcseconds to radians */

    double dmas2r = as2r / 1000.0;	/* milliarcseconds to radians */

    double as2pi = 1296000.0;	/* arc seconds in a full circle */

    double d2pi = 6.283185307179586476925287; /* 2pi */

    double u2r = as2r / 10000000.0;  /* units of 0.1 microarcsecond to radians */

    double dj0 = 2451545.0;	/* reference epoch (j2000), jd */

    double djc = 36525.0;	/* Days per julian century */

    /*  Miscellaneous */
    double t, el, elp, f, d, om, arg, dp, de, sarg, carg;
    double dpsils, depsls, dpsipl, depspl;
    int i, j;

    int nls = NLS; /* number of terms in the luni-solar nutation model */

    /* Fixed offset in lieu of planetary terms (radians) */
    double dpplan = - 0.135 * dmas2r;
    double deplan = + 0.388 * dmas2r;

/* Tables of argument and term coefficients */

    /* Coefficients for fundamental arguments
    /* Luni-solar argument multipliers: */
    /*       l     l'    f     d     om */
static int nals[5*NLS]=
	    {0,    0,    0,    0,    1,
             0,    0,    2,   -2,    2,
             0,    0,    2,    0,    2,
             0,    0,    0,    0,    2,
             0,    1,    0,    0,    0,
             0,    1,    2,   -2,    2,
             1,    0,    0,    0,    0,
             0,    0,    2,    0,    1,
             1,    0,    2,    0,    2,
             0,   -1,    2,   -2,    2,
             0,    0,    2,   -2,    1,
            -1,    0,    2,    0,    2,
            -1,    0,    0,    2,    0,
             1,    0,    0,    0,    1,
            -1,    0,    0,    0,    1,
            -1,    0,    2,    2,    2,
             1,    0,    2,    0,    1,
            -2,    0,    2,    0,    1,
             0,    0,    0,    2,    0,
             0,    0,    2,    2,    2,
             0,   -2,    2,   -2,    2,
            -2,    0,    0,    2,    0,
             2,    0,    2,    0,    2,
             1,    0,    2,   -2,    2,
            -1,    0,    2,    0,    1,
             2,    0,    0,    0,    0,
             0,    0,    2,    0,    0,
             0,    1,    0,    0,    1,
            -1,    0,    0,    2,    1,
             0,    2,    2,   -2,    2,
             0,    0,   -2,    2,    0,
             1,    0,    0,   -2,    1,
             0,   -1,    0,    0,    1,
            -1,    0,    2,    2,    1,
             0,    2,    0,    0,    0,
             1,    0,    2,    2,    2,
            -2,    0,    2,    0,    0,
             0,    1,    2,    0,    2,
             0,    0,    2,    2,    1,
             0,   -1,    2,    0,    2,
             0,    0,    0,    2,    1,
             1,    0,    2,   -2,    1,
             2,    0,    2,   -2,    2,
            -2,    0,    0,    2,    1,
             2,    0,    2,    0,    1,
             0,   -1,    2,   -2,    1,
             0,    0,    0,   -2,    1,
            -1,   -1,    0,    2,    0,
             2,    0,    0,   -2,    1,
             1,    0,    0,    2,    0,
             0,    1,    2,   -2,    1,
             1,   -1,    0,    0,    0,
            -2,    0,    2,    0,    2,
             3,    0,    2,    0,    2,
             0,   -1,    0,    2,    0,
             1,   -1,    2,    0,    2,
             0,    0,    0,    1,    0,
            -1,   -1,    2,    2,    2,
            -1,    0,    2,    0,    0,
             0,   -1,    2,    2,    2,
            -2,    0,    0,    0,    1,
             1,    1,    2,    0,    2,
             2,    0,    0,    0,    1,
            -1,    1,    0,    1,    0,
             1,    1,    0,    0,    0,
             1,    0,    2,    0,    0,
            -1,    0,    2,   -2,    1,
             1,    0,    0,    0,    2,
            -1,    0,    0,    1,    0,
             0,    0,    2,    1,    2,
            -1,    0,    2,    4,    2,
            -1,    1,    0,    1,    1,
             0,   -2,    2,   -2,    1,
             1,    0,    2,    2,    1,
            -2,    0,    2,    2,    2,
            -1,    0,    0,    0,    2,
             1,    1,    2,   -2,    2};

    /* Luni-solar nutation coefficients, in 1e-7 arcsec */
    /* longitude (sin, t*sin, cos), obliquity (cos, t*cos, sin) */
static double cls[6*NLS]=
   {-172064161.0, -174666.0,  33386.0, 92052331.0,  9086.0, 15377.0,
     -13170906.0,   -1675.0, -13696.0,  5730336.0, -3015.0, -4587.0,
      -2276413.0,    -234.0,   2796.0,   978459.0,  -485.0,  1374.0,
       2074554.0,     207.0,   -698.0,  -897492.0,   470.0,  -291.0,
       1475877.0,   -3633.0,  11817.0,    73871.0,  -184.0, -1924.0,
       -516821.0,    1226.0,   -524.0,   224386.0,  -677.0,  -174.0,
        711159.0,      73.0,   -872.0,    -6750.0,     0.0,   358.0,
       -387298.0,    -367.0,    380.0,   200728.0,    18.0,   318.0,
       -301461.0,     -36.0,    816.0,   129025.0,   -63.0,   367.0,
        215829.0,    -494.0,    111.0,   -95929.0,   299.0,   132.0,
        128227.0,     137.0,    181.0,   -68982.0,    -9.0,    39.0,
        123457.0,      11.0,     19.0,   -53311.0,    32.0,    -4.0,
        156994.0,      10.0,   -168.0,    -1235.0,     0.0,    82.0,
         63110.0,      63.0,     27.0,   -33228.0,     0.0,    -9.0,
        -57976.0,     -63.0,   -189.0,    31429.0,     0.0,   -75.0,
        -59641.0,     -11.0,    149.0,    25543.0,   -11.0,    66.0,
        -51613.0,     -42.0,    129.0,    26366.0,     0.0,    78.0,
         45893.0,      50.0,     31.0,   -24236.0,   -10.0,    20.0,
         63384.0,      11.0,   -150.0,    -1220.0,     0.0,    29.0,
        -38571.0,      -1.0,    158.0,    16452.0,   -11.0,    68.0,
         32481.0,       0.0,      0.0,   -13870.0,     0.0,     0.0,
        -47722.0,       0.0,    -18.0,      477.0,     0.0,   -25.0,
        -31046.0,      -1.0,    131.0,    13238.0,   -11.0,    59.0,
         28593.0,       0.0,     -1.0,   -12338.0,    10.0,    -3.0,
         20441.0,      21.0,     10.0,   -10758.0,     0.0,    -3.0,
         29243.0,       0.0,    -74.0,     -609.0,     0.0,    13.0,
         25887.0,       0.0,    -66.0,     -550.0,     0.0,    11.0,
        -14053.0,     -25.0,     79.0,     8551.0,    -2.0,   -45.0,
         15164.0,      10.0,     11.0,    -8001.0,     0.0,    -1.0,
        -15794.0,      72.0,    -16.0,     6850.0,   -42.0,    -5.0,
         21783.0,       0.0,     13.0,     -167.0,     0.0,    13.0,
        -12873.0,     -10.0,    -37.0,     6953.0,     0.0,   -14.0,
        -12654.0,      11.0,     63.0,     6415.0,     0.0,    26.0,
        -10204.0,       0.0,     25.0,     5222.0,     0.0,    15.0,
         16707.0,     -85.0,    -10.0,      168.0,    -1.0,    10.0,
         -7691.0,       0.0,     44.0,     3268.0,     0.0,    19.0,
        -11024.0,       0.0,    -14.0,      104.0,     0.0,     2.0,
          7566.0,     -21.0,    -11.0,    -3250.0,     0.0,    -5.0,
         -6637.0,     -11.0,     25.0,     3353.0,     0.0,    14.0,
         -7141.0,      21.0,      8.0,     3070.0,     0.0,     4.0,
         -6302.0,     -11.0,      2.0,     3272.0,     0.0,     4.0,
          5800.0,      10.0,      2.0,    -3045.0,     0.0,    -1.0,
          6443.0,       0.0,     -7.0,    -2768.0,     0.0,    -4.0,
         -5774.0,     -11.0,    -15.0,     3041.0,     0.0,    -5.0,
         -5350.0,       0.0,     21.0,     2695.0,     0.0,    12.0,
         -4752.0,     -11.0,     -3.0,     2719.0,     0.0,    -3.0,
         -4940.0,     -11.0,    -21.0,     2720.0,     0.0,    -9.0,
          7350.0,       0.0,     -8.0,      -51.0,     0.0,     4.0,
          4065.0,       0.0,      6.0,    -2206.0,     0.0,     1.0,
          6579.0,       0.0,    -24.0,     -199.0,     0.0,     2.0,
          3579.0,       0.0,      5.0,    -1900.0,     0.0,     1.0,
          4725.0,       0.0,     -6.0,      -41.0,     0.0,     3.0,
         -3075.0,       0.0,     -2.0,     1313.0,     0.0,    -1.0,
         -2904.0,       0.0,     15.0,     1233.0,     0.0,     7.0,
          4348.0,       0.0,    -10.0,      -81.0,     0.0,     2.0,
         -2878.0,       0.0,      8.0,     1232.0,     0.0,     4.0,
         -4230.0,       0.0,      5.0,      -20.0,     0.0,    -2.0,
         -2819.0,       0.0,      7.0,     1207.0,     0.0,     3.0,
         -4056.0,       0.0,      5.0,       40.0,     0.0,    -2.0,
         -2647.0,       0.0,     11.0,     1129.0,     0.0,     5.0,
         -2294.0,       0.0,    -10.0,     1266.0,     0.0,    -4.0,
          2481.0,       0.0,     -7.0,    -1062.0,     0.0,    -3.0,
          2179.0,       0.0,     -2.0,    -1129.0,     0.0,    -2.0,
          3276.0,       0.0,      1.0,       -9.0,     0.0,     0.0,
         -3389.0,       0.0,      5.0,       35.0,     0.0,    -2.0,
          3339.0,       0.0,    -13.0,     -107.0,     0.0,     1.0,
         -1987.0,       0.0,     -6.0,     1073.0,     0.0,    -2.0,
         -1981.0,       0.0,      0.0,      854.0,     0.0,     0.0,
          4026.0,       0.0,   -353.0,     -553.0,     0.0,  -139.0,
          1660.0,       0.0,     -5.0,     -710.0,     0.0,    -2.0,
         -1521.0,       0.0,      9.0,      647.0,     0.0,     4.0,
          1314.0,       0.0,      0.0,     -700.0,     0.0,     0.0,
         -1283.0,       0.0,      0.0,      672.0,     0.0,     0.0,
         -1331.0,       0.0,      8.0,      663.0,     0.0,     4.0,
          1383.0,       0.0,     -2.0,     -594.0,     0.0,    -2.0,
          1405.0,       0.0,      4.0,     -610.0,     0.0,     2.0,
          1290.0,       0.0,      0.0,     -556.0,     0.0,     0.0};

    /* Interval between fundamental epoch J2000.0 and given date (JC) */
    t = (dj - dj0) / djc;

/* Luni-solar nutation */

/* Fundamental (delaunay) arguments from Simon et al. (1994) */

    /* Mean anomaly of the moon */
    el  = fmod (485868.249036 + (1717915923.2178 * t), as2pi) * as2r;

    /* Mean anomaly of the sun */
    elp = fmod (1287104.79305 + (129596581.0481 * t), as2pi) * as2r;

    /* Mean argument of the latitude of the moon */
    f   = fmod (335779.526232 + (1739527262.8478 * t), as2pi) * as2r;

    /* Mean elongation of the moon from the sun */
    d   = fmod (1072260.70369 + (1602961601.2090 * t), as2pi ) * as2r;

    /* Mean longitude of the ascending node of the moon */
    om  = fmod (450160.398036 - (6962890.5431 * t), as2pi ) * as2r;

    /* Initialize the nutation values */
    dp = 0.0;
    de = 0.0;

    /* Summation of luni-solar nutation series (in reverse order) */
    for (i = nls; i > 0; i=i-1) {
	j = i - 1;

	/* Argument and functions */
	arg = fmod ( (double) (nals[5*j]) * el +
		     (double) (nals[1+5*j]) * elp +
		     (double) (nals[2+5*j]) * f +
		     (double) (nals[3+5*j]) * d +
		     (double) (nals[4+5*j]) * om, d2pi);
	sarg = sin (arg);
	carg = cos (arg);

	/* Terms */
	dp = dp + (cls[6*j] + cls[1+6*j] * t) * sarg + cls[2+6*j] * carg;
	de = de + (cls[3+6*j] + cls[4+6*j] * t) * carg + cls[5+6*j] * sarg;
	}

    /* Convert from 0.1 microarcsec units to radians */
    dpsils = dp * u2r;
    depsls = de * u2r;

/* In lieu of planetary nutation */

    /* Fixed offset to correct for missing terms in truncated series */
    dpsipl = dpplan;
    depspl = deplan;

/* Results */

    /* Add luni-solar and planetary components */
    *dpsi = dpsils + dpsipl;
    *deps = depsls + depspl;

    /* Mean Obliquity in radians (IAU 2006, Hilton, et al.) */
    *eps0 = ( 84381.406     +
	    ( -46.836769    +
	    (  -0.0001831   +
	    (   0.00200340  +
	    (  -0.000000576 +
	    (  -0.0000000434 ) * t ) * t ) * t ) * t ) * t ) * as2r;
}
