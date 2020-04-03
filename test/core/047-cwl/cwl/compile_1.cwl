#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: /usr/bin/gcc
arguments: ["-std=c++11", "-o", "source_1.o"]
inputs:
    src:
        type: File
        inputBinding:
            prefix: -c
            separate: true
            position: 1

outputs:
    object_file:
        type: File
        outputBinding:
            glob: "source_1.o"
