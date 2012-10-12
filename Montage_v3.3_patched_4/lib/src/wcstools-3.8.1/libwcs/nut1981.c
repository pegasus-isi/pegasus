

/* Compute the longitude and obliquity components of nutation and
 * mean obliquity from the IAU 1980 theory
 * References:
 *    Final Report of the IAU Working Group on Nutation,
 *    Chairman P.K.Seidelmann, 1980.
 *    Kaplan,G.H., 1981, USNO Circular No. 163, pa3-6.
 *
 * From Fortran code by P.T. Wallace   Starlink   september 1987
 */

void
compnut (dj, dpsi, deps, eps0)

double dj;	/* TDB (loosely ET or TT) as Julian Date */
double *dpsi;	/* Nutation in longitude in radians (returned) */
double *deps;	/* Nutation in obliquity in radians (returned) */
double *eps0;	/* Mean obliquity in radians (returned) */
{
    double t2as,as2r,u2r;
    double t,el,el2,el3;
    double elp,elp2;
    double f,f2,f4;
    double d,d2,d4;
    double om,om2;
    double dp,de;
    double a;

    /* Turns to arc seconds */
    t2as = 1296000.0;

    /* Arc seconds to radians */
    as2r = 0.000004848136811095359949;

    /* Units of 0.0001 arcsec to radians */
    u2r = as2r / 10000.0;

    /* Basic epoch J2000.0 to current epoch in Julian Centuries */
    t = (dj - 2400000.5  - 51544.5 ) / 36525.0;

    /* Fundamental arguments in the FK5 reference system */

    /* mean longitude of the moon minus mean longitude of the moon's perigee */
    el = as2r*(485866.733 + (1325.0 * t2as+715922.633 + (31.310 +0.064*t)*t)*t);

    /* mean longitude of the sun minus mean longitude of the sun's perigee */
    elp = as2r*(1287099.804 + (99.0 * t2as+1292581.224 + (-0.577 -0.012*t)*t)*t);

    /* mean longitude of the moon minus mean longitude of the moon's node */
    f = as2r*(335778.877 + (1342.0 * t2as+295263.137 + (-13.257 + 0.011*t)*t)*t);

    /* mean elongation of the moon from the sun */
    d = as2r*(1072261.307 + (1236.0 * t2as+1105601.328 + (-6.891 + 0.019*t)*t)*t);

    /* longitude of the mean ascending node of the lunar orbit on the */
    /*  ecliptic, measured from the mean equinox of date */
    om = as2r * (450160.280 + (-5.0 * t2as-482890.539 + (7.455 +0.008*t)*t)*t);

    /* Multiples of arguments */
    el2 = el + el;
    el3 = el2 + el;
    elp2 = elp + elp;
    f2 = f + f;
    f4 = f2 + f2;
    d2 = d + d;
    d4 = d2 + d2;
    om2 = om + om;

    /* Series for the nutation */
    dp = 0.0;
    de = 0.0;

    /* 106 */
    dp = dp + sin (elp+d);

    /* 105 */
    dp = dp - sin (f2 + d4 + om2);

    /* 104 */
    dp = dp + sin (el2 + d2);

    /* 103 */
    dp = dp - sin (el - f2 + d2);

    /* 102 */
    dp = dp - sin (el + elp - d2 + om);

    /* 101 */
    dp = dp - sin (-elp + f2 + om);

    /* 100 */
    dp = dp - sin (el - f2 - d2);

    /* 99 */
    dp = dp - sin (elp + d2);

    /* 98 */
    dp = dp - sin (f2 - d + om2);

    /* 97 */
    dp = dp - sin (-f2 + om);

    /* 96 */
    dp = dp + sin (-el - elp + d2 + om);

    /* 95 */
    dp = dp + sin (elp + f2 + om);

    /* 94 */
    dp = dp - sin (el + f2 - d2);

    /* 93 */
    dp = dp + sin(el3 + f2 - d2 + om2);

    /* 92 */
    dp = dp + sin(f4 - d2 + om2);

    /* 91 */
    dp = dp - sin(el + d2 + om);

    /* 90 */
    dp = dp - sin(el2 + f2 + d2 + om2);

    /* 89 */
    a = el2 + f2 - d2 + om;
    dp = dp + sin(a);
    de = de - cos(a);

    /* 88 */
    dp = dp + (sin(el - elp - d2));

    /* 87 */
    dp = dp + (sin(-el + f4 + om2));

    /* 86 */
    a = -el2 + f2 + d4 + om2;
    dp = dp - sin(a);
    de = de + cos(a);

    /* 85 */
    a = el + f2 + d2 + om;
    dp = dp - sin(a);
    de = de + cos(a);

    /* 84 */
    a = el + elp + f2 - d2 + om2;
    dp = dp + sin(a);
    de = de - cos(a);

    /* 83 */
    dp = dp - sin(el2 - d4);

    /* 82 */
    a = -el + f2 + d4 + om2;
    dp = dp - (2.0 * sin(a));
    de = de + cos(a);

    /* 81 */
    a = -el2 + f2 + d2 + om2;
    dp = dp + sin(a);
    de = de - cos(a);

    /* 80 */
    dp = dp - sin(el - d4);

    /* 79 */
    a = -el + om2;
    dp = dp + sin(a);
    de = de - cos(a);

    /* 78 */
    a = f2 + d + om2;
    dp = dp + (2.0 * sin(a));
    de = de - cos(a);

    /* 77 */
    dp = dp + (2.0 * sin(el3));

    /* 76 */
    a = el + om2;
    dp = dp - (2.0 * sin(a));
    de = de + cos(a);

    /* 75 */
    a = el2 + om;
    dp = dp + (2.0 * sin(a));
    de = de - cos(a);

    /* 74 */
    a =  - el + f2 - d2 + om;
    dp = dp - (2.0 * sin(a));
    de = de + cos(a);

    /* 73 */
    a = el + elp + f2 + om2;
    dp = dp + (2.0 * sin(a));
    de = de - cos(a);

    /* 72 */
    a = -elp + f2 + d2 + om2;
    dp = dp - (3.0 * sin(a));
    de = de + cos(a);

    /* 71 */
    a = el3 + f2 + om2;
    dp = dp - (3.0 * sin(a));
    de = de + cos(a);

    /* 70 */
    a = -el2 + om;
    dp = dp - (2.0 * sin(a));
    de = de + cos(a);

    /* 69 */
    a = -el - elp + f2 + d2 + om2;
    dp = dp - (3.0 * sin(a));
    de = de + cos(a);

    /* 68 */
    a = el - elp + f2 + om2;
    dp = dp - (3.0 * sin(a));
    de = de + cos(a);

    /* 67 */
    dp = dp + (3.0 * sin(el + f2));

    /* 66 */
    dp = dp - (3.0 * sin(el + elp));

    /* 65 */
    dp = dp - (4.0 * sin(d));

    /* 64 */
    dp = dp + (4.0 * sin(el - f2));

    /* 63 */
    dp = dp - (4.0 * sin(elp - d2));

    /* 62 */
    a = el2 + f2 + om;
    dp = dp - (5.0 * sin(a));
    de = de + (3.0 * cos(a));

    /* 61 */
    dp = dp + (5.0 * sin(el - elp));

    /* 60 */
    a = -d2 + om;
    dp = dp - (5.0 * sin(a));
    de = de + (3.0 * cos(a));

    /* 59 */
    a = el + f2 - d2 + om;
    dp = dp + (6.0 * sin(a));
    de = de - (3.0 * cos(a));

    /* 58 */
    a = f2 + d2 + om;
    dp = dp - (7.0 * sin(a));
    de = de + (3.0 * cos(a));

    /* 57 */
    a = d2 + om;
    dp = dp - (6.0 * sin(a));
    de = de + (3.0 * cos(a));

    /* 56 */
    a = el2 + f2 - d2 + om2;
    dp = dp + (6.0 * sin(a));
    de = de - (3.0 * cos(a));

    /* 55 */
    dp = dp + (6.0 * sin(el + d2));

    /* 54 */
    a = el + f2 + d2 + om2;
    dp = dp - (8.0 * sin(a));
    de = de + (3.0 * cos(a));

    /* 53 */
    a = -elp + f2 + om2;
    dp = dp - (7.0 * sin(a));
    de = de + (3.0 * cos(a));

    /* 52 */
    a = elp + f2 + om2;
    dp = dp + (7.0 * sin(a));
    de = de - (3.0 * cos(a));

    /* 51 */
    dp = dp - (7.0 * sin(el + elp - d2));

    /* 50 */
    a = -el + f2 + d2 + om;
    dp = dp - (10.0 * sin(a));
    de = de + (5.0 * cos(a));

    /* 49 */
    a = el - d2 + om;
    dp = dp - (13.0 * sin(a));
    de = de + (7.0 * cos(a));

    /* 48 */
    a = -el + d2 + om;
    dp = dp + (16.0 * sin(a));
    de = de - (8.0 * cos(a));

    /* 47 */
    a =  - el + f2 + om;
    dp = dp + (21.0 * sin(a));
    de = de - (10.0 * cos(a));

    /* 46 */
    dp = dp + (26.0 * sin(f2));
    de = de - cos(f2);

    /* 45 */
    a = el2 + f2 + om2;
    dp = dp - (31.0 * sin(a));
    de = de + (13.0 * cos(a));

    /* 44 */
    a = el + f2 - d2 + om2;
    dp = dp + (29.0 * sin(a));
    de = de - (12.0 * cos(a));

    /* 43 */
    dp = dp + (29.0 * sin(el2));
    de = de - cos(el2);

    /* 42 */
    a = f2 + d2 + om2;
    dp = dp - (38.0 * sin(a));
    de = de + (16.0 * cos(a));

    /* 41 */
    a = el + f2 + om;
    dp = dp - (51.0 * sin(a));
    de = de + (27.0 * cos(a));

    /* 40 */
    a =  -el + f2 + d2 + om2;
    dp = dp - (59.0 * sin(a));
    de = de + (26.0 * cos(a));

    /* 39 */
    a =  -el + om;
    dp = dp + ((-58.0 - 0.1 * t) * sin(a));
    de = de + (32.0 * cos(a));

    /* 38 */
    a = el + om;
    dp = dp + ((63.0 + 0.1 * t) * sin(a));
    de = de - (33.0 * cos(a));

    /* 37 */
    dp = dp + (63.0 * sin(d2));
    de = de - (2.0 * cos(d2));

    /* 36 */
    a =  -el + f2 + om2;
    dp = dp + (123.0 * sin(a));
    de = de - (53.0 * cos(a));

    /* 35 */
    a = el - d2;
    dp = dp - (158.0 * sin(a));
    de = de - cos(a);

    /* 34 */
    a = el + f2 + om2;
    dp = dp - (301.0 * sin(a));
    de = de + ((129.0 - 0.1 * t) * cos(a));

    /* 33 */
    a = f2 + om;
    dp = dp + ((-386.0  - 0.4 * t) * sin(a));
    de = de + (200.0 * cos(a));

    /* 32 */
    dp = dp + ((712.0  + 0.1 * t) * sin(el));
    de = de - (7.0 * cos(el));

    /* 31 */
    a = f2 + om2;
    dp = dp + ((-2274.0  - 0.2 * t) * sin(a));
    de = de + ((977.0  - 0.5 * t) * cos(a));

    /* 30 */
    dp = dp - sin(elp + f2 - d2);

    /* 29 */
    dp = dp + sin(-el + d + om);

    /* 28 */
    dp = dp + sin(elp + om2);

    /* 27 */
    dp = dp - sin(elp - f2 + d2);

    /* 26 */
    dp = dp + sin(-f2 + d2 + om);

    /* 25 */
    dp = dp + sin(el2 + elp - d2);

    /* 24 */
    dp = dp - (4.0 * sin(el - d));

    /* 23 */
    a = elp + f2 - d2 + om;
    dp = dp + (4.0 * sin(a));
    de = de - (2.0 * cos(a));

    /* 22 */
    a = el2 - d2 + om;
    dp = dp + (4.0 * sin(a));
    de = de - (2.0 * cos(a));

    /* 21 */
    a = -elp + f2 - d2 + om;
    dp = dp - (5.0 * sin(a));
    de = de + (3.0 * cos(a));

    /* 20 */
    a = -el2 + d2 + om;
    dp = dp - (6.0 * sin(a));
    de = de + (3.0 * cos(a));

    /* 19 */
    a = -elp + om;
    dp = dp - (12.0 * sin(a));
    de = de + (6.0 * cos(a));

    /* 18 */
    a = elp2 + f2 - d2 + om2;
    dp = dp + ((-16.0  + (0.1 * t)) * sin(a));
    de = de + (7.0 * cos(a));

    /* 17 */
    a = elp + om;
    dp = dp - (15.0 * sin(a));
    de = de + (9.0 * cos(a));

    /* 16 */
    dp = dp + ((17.0  - (0.1 * t)) * sin(elp2));

    /* 15 */
    dp = dp - (22.0 * sin(f2 - d2));

    /* 14 */
    a = el2 - d2;
    dp = dp + (48.0 * sin(a));
    de = de + cos(a);

    /* 13 */
    a = f2 - d2 + om;
    dp = dp + ((129.0 + (0.1 * t)) * sin(a));
    de = de - (70.0 * cos(a));

    /* 12 */
    a =  - elp + f2 - d2 + om2;
    dp = dp + ((217.0  - 0.5 * t) * sin(a));
    de = de + ((-95.0  + 0.3 * t) * cos(a));

    /* 11 */
    a = elp + f2 - d2 + om2;
    dp = dp + ((-517.0  + (1.2 * t)) * sin(a));
    de = de + ((224.0  - (0.6 * t)) * cos(a));

    /* 10 */
    dp = dp + ((1426.0 - (3.4 * t)) * sin(elp));
    de = de + ((54.0 - (0.1 * t)) * cos(elp));

    /* 9 */
    a = f2 - d2 + om2;
    dp = dp + ((-13187.0  - (1.6 * t)) * sin(a));
    de = de + ((5736.0  - (3.1 * t)) * cos(a));

    /* 8 */
    dp = dp + sin(el2 - f2 + om);

    /* 7 */
    a =  -elp2 + f2 - d2 + om;
    dp = dp - (2.0 * sin(a));
    de = de + (1.0 * cos(a));

    /* 6 */
    dp = dp - (3.0 * sin(el - elp - d));

    /* 5 */
    a =  - el2 + f2 + om2;
    dp = dp - (3.0 * sin(a));
    de = de + (1.0 * cos(a));

    /* 4 */
    dp = dp + (11.0 * sin (el2 - f2));

    /* 3 */
    a =  - el2 + f2 + om;
    dp = dp + (46.0 * sin(a));
    de = de - (24.0 * cos(a));

    /*  2 */
    dp = dp + ((2062.0 + (0.2 * t)) * sin(om2));
    de = de + ((-895.0 + (0.5 * t)) * cos(om2));

    /* 1 */
    dp = dp + ((-171996.0 - (174.2 * t)) * sin(om));
    de = de + ((92025.0 + (8.9 * t)) * cos(om));

    /* Convert results to radians */
    *dpsi = dp * u2r;
    *deps = de * u2r;

    /* Mean Obliquity in radians */
    *eps0 = as2r * (84381.448 + (-46.8150 + (-0.00059 + (0.001813*t)*t)*t));

    return;
}
