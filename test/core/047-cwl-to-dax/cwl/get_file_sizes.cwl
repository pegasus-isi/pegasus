#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: CommandLineTool
baseCommand: get_file_sizes.sh
inputs:
    files:
        type: File[]
        inputBinding:
            position: 0

outputs:
    file_sizes:
        type: File
        outputBinding:
            glob: "file_sizes.txt"
