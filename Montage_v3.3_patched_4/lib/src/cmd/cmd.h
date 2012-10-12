/**
    \file       cmd.h
    \author     <a href="mailto:jcg@ipac.caltech.edu">John Good</a>
 */

/**
    \mainpage   libcmd: Command String Parsing Library
 
    <hr>
    <center><h2>Command String Parsing Library</h2></center>
    <p>

    <center><b>Table of Contents</b></a>
    <table border=1 cellpadding=2 width=75%>
        <tr><td>
            <center><table><tr><td>
                <ul>
                <li><a href=#description>   General Description</a></li>
                <li><a href=#control>       Library Routines</a></li>
                </ul>
            </td></tr></table></center>
        </td></tr>
    </table>
    </center>
 
    <a name=description><h2>General Description</h2></a>

    <p>
    Many of the ISIS services are interactive, command-driven programs in
    their own right. The normal command mode is the standard verb /
    argument-list syntax (as exemplified by the UNIX <tt>argc, argv[]</tt>
    construct).

    <p>
    This library takes a string (the command line) and returns an argument count 
    and set of substrings corresponding to individual arguments.

    <p><hr><p>

    <a name=control><h2>Library Routines</h2></a>

    <p>
    The calls in the <tt><b>cmd</b></tt> library are as follows:

    <p>
    <ul>
    <li>setwhitespace(const char*)</li>
    <li>isws(char)</li>
    <li>parsecmd(char *, char **)</li>
    </ul>

    <p>
    This routine has the same functionality as the system uses to set up
    <tt>argc, argv[]</tt> for process execution.  The user passes the routine 
    a string and a set of pointers that will be set to point to the extracted
    substrings. The routine returns the number of substrings extracted. Quoted 
    strings are treated as a single unit, with the standard escape sequence for 
    embedded quotes (i.e. <tt>\"</tt>).

    <p>
    Note that the contents of the input string are changed, and that the caller
    is responsible for providing an array of pointers large enough to hold all
    extracted substrings.
 
    <p><hr><p>

    \author     <a href="mailto:jcg@ipac.caltech.edu">John Good</a>
 */


#ifndef ISIS_CMD_LIB
#define ISIS_CMD_LIB

/**
    Sets the characters which are to be considered as whitespace by isws(char)
    to the characters of the string <tt>wsin</tt> (the NULL character cannot be
    considered as whitespace. If <tt>wsin</tt> is NULL, then a default character 
    set is instated.

    \param wsin		the characters to be considered as whitespace by isws(char).
 */
void setwhitespace(const char * const wsin);

/**
    Tests to see if the character <tt>ch</tt> is a whitespace
    character, as defined by the last call to setwhitespace(const char *) or
    the default character set.

    \param ch	the character to test
    \return	A non-zero value if <tt>ch</tt> is determined to be a whitespace
		character, zero otherwise.
 */
int isws(char ch);

/**
    Splits the command string <tt>cmd</tt> into pieces.

    <p>
    The input string (<tt>cmd</tt>) is parsed, and a set of pointers 
    to the resulting subtrings is stored in <tt>cmdv</tt>. The number
    of generated substrings is returned. Strings enclosed in double quotes 
    (<tt>"</tt>) are treated as a single unit, and the standard escape syntax 
    (i.e. <tt>\"</tt>) is observed. Note that the contents of <tt>cmd</tt>
    are modified during the parsing process: whitespace characters are replaced
    with null terminators. The <tt>setwhitespace(const char*)</tt> function can be
    called to specify which characters should be considered as whitespace 
    (i.e. which characters are substring separators). 
    </p>
    <p>
    CAVEATS:
    <ul>
    <li>This function is <em>not</em> reentrant</li>
    <li>The user is responsible for ensuring that <tt>cmdv</tt> is large
        enough to hold a pointer for every substring. If <tt>n</tt> is the 
        length of <tt>cmd</tt>, then an array of <tt>ceil(n/2)</tt> pointers
        is guaranteed to suffice.</li>
    </ul>
    </p>

    \param cmd		the string to split
    \param cmdv		an array of pointers to the substrings of <tt>cmd</tt>
    \return		the number of substrings <tt>cmd</tt> was split into.
 */
int parsecmd(char * cmd, char ** cmdv);

#endif /* ISIS_CMD_LIB */

