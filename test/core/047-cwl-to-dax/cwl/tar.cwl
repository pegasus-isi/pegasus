#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: /usr/bin/tar
arguments: ["--strip", "1", "-xzvf"]
inputs:
    to_extract:
        type: string[]
        inputBinding:
            position: 1

    tar_file:
        type: File
        inputBinding:
            position: 0

outputs:
    source_file_1:
        type: File
        outputBinding:
            glob: "source_1.cpp"

    source_file_2:
        type: File
        outputBinding:
            glob: "source_2.cpp"
