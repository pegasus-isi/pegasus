#ifndef STRLIB_H
#define STRLIB_H

#include <vector>
#include <string>

void trim(std::string &str, const std::string &delim = " \t\r\n");

void split(std::vector<std::string> &v, const std::string &line, const std::string &delim = " \t\r\n", unsigned maxsplits = 0);

void split_args(std::vector<std::string> &v, const std::string &line);


#endif /* STRLIB_H */
