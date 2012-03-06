#include <string>
#include <vector>
#include "stdio.h"

#include "strlib.h"
#include "failure.h"

void test_trim(std::string &before, const std::string &after, const std::string &delim = " \t\r\n") {
    trim(before, delim);
    
    if (before.compare(after) != 0) {
        failure("Trim didn't work: '%s' != '%s'", before.c_str(), after.c_str());
    }
}

void test_split_args(const std::string &arg, const std::vector<std::string> &result) {
    std::vector<std::string> args;
    
    split_args(args, arg);
    
    if (result.size() != args.size()) {
        failure("Size mismatch");
    }
    
    for (unsigned i=0; i<args.size(); i++) {
        std::string l = args[i];
        std::string r = result[i];
        if (l.compare(r) != 0) {
            failure("Strings don't match: '%s' != '%s'", l.c_str(), r.c_str());
        }
    }
}

void test_split(const std::string &arg, const std::vector<std::string> &result, const std::string &delim = " \t\r\n", unsigned max = 0) {
    std::vector<std::string> v;
    
    split(v, arg, delim, max);
    
    if (result.size() != v.size()) {
        failure("Size mismatch: %d != %d", (unsigned)result.size(), (unsigned)v.size());
    }
    
    for (unsigned i = 0; i < v.size(); i++) {
        std::string l = v[i];
        std::string r = result[i];
        if (l.compare(r) != 0) {
            failure("Strings don't match: '%s' != '%s'", l.c_str(), r.c_str());
        }
    }
}

int main(int argc, char *argv[]) {
    std::vector<std::string> v;
    
    // SPLIT
    v.push_back("foo");
    test_split("foo", v);
    v.clear();
    
    v.push_back("foo");
    v.push_back("bar");
    test_split("foo   bar", v);
    v.clear();
    
    v.push_back("foo");
    v.push_back("bar baz");
    test_split("foo bar baz", v, " ", 1);
    v.clear();
    
    v.push_back("foo");
    v.push_back("bar");
    test_split("foo bar", v, " ", 1);
    v.clear();
    
    v.push_back("foo");
    v.push_back("bar baz");
    test_split("foo bar baz", v, " ", 1);
    v.clear();
    
    v.push_back("foo");
    v.push_back("bar");
    test_split("foo,bar", v, ",");
    v.clear();
    
    v.push_back("foo");
    v.push_back("bar baz");
    test_split("foo    bar baz", v, " ", 1);
    v.clear();
    
    // SPLIT ARGS
    v.push_back("foo");
    v.push_back("bar");
    test_split_args("foo bar", v);
    v.clear();
    
    v.push_back("a b c");
    test_split_args("\"a b c\"", v);
    v.clear();
    
    v.push_back("first one");
    v.push_back("second one");
    test_split_args("\"first one\" \'second one\'", v);
    v.clear();
    
    v.push_back("unterminated quoted string");
    test_split_args("\"unterminated quoted string", v);
    v.clear();
    
    v.push_back("with \" quote");
    test_split_args("\"with \\\" quote\"", v);
    v.clear();
    
    v.push_back("escapeatend\\");
    test_split_args("escapeatend\\", v);
    v.clear();
    
    v.push_back("escapeb");
    test_split_args("escape\\b", v);
    v.clear();
    
    std::string str;
    
    str += "   abc";
    test_trim(str, "abc");
    str.clear();
    
    str += "abc   ";
    test_trim(str, "abc");
    str.clear();
    
    str += "   abc   ";
    test_trim(str, "abc");
    str.clear();
    
    str += "abc";
    test_trim(str, "abc");
    str.clear();
    
    str += "abc\t\n";
    test_trim(str, "abc");
    str.clear();
    
    str += "dabcd";
    test_trim(str, "abc", "d");
    str.clear();
    
    test_trim(str, "");
    str.clear();
    
    str += "\n";
    test_trim(str, "");
    str.clear();
    
    return 0;
}
