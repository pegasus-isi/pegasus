#ifndef STRLIB_H
#define STRLIB_H

#include <vector>
#include <list>
#include <string>

using std::vector;
using std::list;
using std::string;

void trim(string &str, const string &delim = " \t\r\n");

void split(vector<string> &v, const string &line, const string &delim = " \t\r\n", unsigned maxsplits = 0);

void split_args(list<string> &v, const string &line);


#endif /* STRLIB_H */
