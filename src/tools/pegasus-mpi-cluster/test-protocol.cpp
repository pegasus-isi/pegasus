#include <string>
#include <stdio.h>

#include "protocol.h"
#include "failure.h"
#include "log.h"

using std::exception;

char *msgcopy(char *msg, int msgsize) {
    char *message = (char *)malloc(msgsize);
    memcpy(message, msg, msgsize);
    return message;
}

void test_command() {
    string name = "name";
    string command = "command";
    string id = "id";
    unsigned memory = 1;
    unsigned cpus = 2;
    map<string,string> pipe_forwards;
    pipe_forwards["FOO"] = "BAR";
    map<string,string> file_forwards;
    file_forwards["BAZ"] = "BOO";
    CommandMessage input(name, command, id, memory, cpus, &pipe_forwards, &file_forwards);
    CommandMessage output(msgcopy(input.msg, input.msgsize), input.msgsize, 0);
    if (input.name != output.name) {
        myfailure("names don't match");
    }
    if (input.command != output.command) {
        myfailure("commands don't match");
    }
    if (input.id != output.id) {
        myfailure("ids don't match");
    }
    if (input.memory != output.memory) {
        myfailure("memories don't match");
    }
    if (input.cpus != output.cpus) {
        myfailure("cpus don't match");
    }
    if (output.pipe_forwards["FOO"] != input.pipe_forwards["FOO"]) {
        myfailure("pipe forwards don't match");
    }
    if (output.file_forwards["BAZ"] != input.file_forwards["BAZ"]) {
        myfailure("file forwards don't match");
    }
}

void test_result() {
    string name = "name";
    int exitcode = 127;
    double runtime = 123.456;
    ResultMessage input(name, exitcode, runtime);
    ResultMessage output(msgcopy(input.msg, input.msgsize), input.msgsize, 0, 0);
    if (output.name != input.name) {
        myfailure("name does not match");
    }
    if (output.exitcode != input.exitcode) {
        myfailure("exitcode does not match");
    }
    if (output.runtime != input.runtime) {
        myfailure("runtime does not match");
    }
}

void test_shutdown() {
    ShutdownMessage input;
    ShutdownMessage output(msgcopy(input.msg, input.msgsize), input.msgsize, 0);
}

void test_registration() {
    string hostname = "hostname";
    unsigned memory = 7;
    unsigned cpus = 5;
    RegistrationMessage input(hostname, memory, cpus);
    RegistrationMessage output(msgcopy(input.msg, input.msgsize), input.msgsize, 0);
    if (input.hostname != output.hostname) {
        myfailure("hostname does not match");
    }
    if (input.memory != output.memory) {
        myfailure("memory does not match");
    }
    if (input.cpus != output.cpus) {
        myfailure("cpus do not match");
    }
}

void test_hostrank() {
    int hostrank = 17;
    HostrankMessage input(hostrank);
    HostrankMessage output(msgcopy(input.msg, input.msgsize), input.msgsize, 0);
    if (input.hostrank != output.hostrank) {
        myfailure("hostrank does not match");
    }
}

void test_iodata() {
    string task = "task";
    string filename = "filename";
    string data = "this is data";
    unsigned size = data.size();
    IODataMessage input(task, filename, data.c_str(), size);
    IODataMessage output(msgcopy(input.msg, input.msgsize), input.msgsize, 0);

    if (input.task != output.task) {
        myfailure("task does not match");
    }
    if (input.filename != output.filename) {
        myfailure("filename does not match");
    }
    if (input.size != output.size) {
        myfailure("size does not match");
    }
    if (strncmp(input.data, output.data, size)) {
        myfailure("data does not match");
    }
}

int main(int argc, char *argv[]) {
    try {
        log_set_level(LOG_ERROR);
        test_command();
        test_result();
        test_shutdown();
        test_registration();
        test_hostrank();
        test_iodata();
        return 0;
    } catch (exception &error) {
        log_error("ERROR: %s", error.what());
        return 1;
    }
}
