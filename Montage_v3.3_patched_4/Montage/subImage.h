/* Module: subImage.h

Version  Developer        Date     Change
-------  ---------------  -------  -----------------------
1.0      Loring Craymer   20Jan05  Baseline code

*/

struct imageParams {
	int ibegin;		/* column offset */
	int iend;		/* last column */
	int jbegin;		/* row offset */
	int jend;		/* last row */
	long nelements;		/* row length */
	int nfound;
	int isDSS;
	double crpix[10];
	double cnpix[10];
	long naxis;
	long naxes[10];
};
	
	
/* montage_getFileInfo()
 * Extracts parameters from a fits file for processing
 */
extern struct WorldCoor *montage_getFileInfo(fitsfile *infptr, char **header, struct imageParams *params);

/* montage_copyHeader()
 * Copies header information from an input file to an output file, with
 * modifications to match the subimage that is being extracted.
 */
extern int montage_copyHeaderInfo(fitsfile *infptr, fitsfile *outfptr, struct imageParams *params);

/* montage_copyData()
 * Copies subimage data from the input file to the output file.
 */
extern void montage_copyData(fitsfile *infptr, fitsfile *outfptr, struct imageParams *params);

/* montage_dataRange()
 * Finds the range of pixels with real data.
 */
extern void montage_dataRange(fitsfile *infptr, int *imin, int *imax, int *jmin, int *jmax);

/* montage_printFitsError()
 * Error reporting function.
 */
extern void montage_printFitsError(int);


