#include "strlib.h"

/**
 * Trim all characters in delim off of both ends of str
 */
void trim(std::string &str, const std::string &delim) {
    if (str.length() == 0) {
        return;
    }
    
    // Start
    unsigned i = 0;
    while (i < str.length()) {
        if (delim.find(str[i]) == delim.npos) {
            break;
        }
        i++;
    }
    
    if (i > 0) {
        str.erase(0, i);
    }
    
    if (str.length() == 0) {
        return;
    }
    
    // End
    unsigned j = str.length() - 1;
    while (j > 0) {
        if (delim.find(str[j]) == delim.npos) {
            break;
        }
        j--;
    }
    
    if (j < str.length() - 1) {
        str.erase(j+1);
    }
}

/**
 * Split line on delim a maximum of maxsplits times and store the result in v
 */
void split(std::vector<std::string> &v, const std::string &line, const std::string &delim, unsigned maxsplits) {
    std::string arg;
    for (unsigned i=0; i<line.length(); i++) {
        char c = line[i];
        if ((maxsplits != 0 && v.size() == maxsplits) || delim.find(c) == delim.npos) {
            arg += c;
        } else {
            if (arg.length() > 0) {
                v.push_back(arg);
                arg.clear();
            }
        }
    }
    
    // Last one
    if (arg.length() > 0) {
        trim(arg, delim);
        v.push_back(arg);
    }
}

/**
 * Split line using command-line argument splitting and store the result in args
 */
void split_args(std::list<std::string> &args, const std::string &line) {
    std::string arg;
    bool inquote = false;
    for (unsigned i=0; i<line.length(); i++) {
        char c = line[i];
        switch (c) {
            case '\'':
            case '"':
                if (inquote) {
                    args.push_back(arg);
                    arg.clear();
                    inquote = false;
                } else {
                    inquote = true;
                }
                break;
            case '\\':
                if (line.length() > i+1) {
                    char next = line[i+1];
                    i++; // Skip one
                    arg += next;
                } else {
                    arg += c;
                }
                break;
            case ' ':
            case '\t':
                if (inquote) {
                    arg += c;
                } else {
                    if (arg.length() > 0) {
                        args.push_back(arg);
                        arg.clear();
                    }
                }
                break;
            default:
                arg += c;
                break;
        }
    }
    
    if (arg.length() > 0) {
        args.push_back(arg);
    }
}
