#!/usr/bin/env cwl-runner
cwlVersion: v1.0
class: Workflow
inputs:
    tarball: File
    filenames_to_extract: string[]

outputs:
    object_file_sizes:
        type: File
        outputSource: get_sizes/file_sizes

steps:
    untar:
        run: tar.cwl
        in:
            tar_file: tarball
            to_extract: filenames_to_extract
        out: [source_file_1, source_file_2]

    compile_1:
        run: compile_1.cwl
        in:
            src: untar/source_file_1
        out: [object_file]

    compile_2:
        run: compile_2.cwl
        in:
            src: untar/source_file_2
        out: [object_file]

    get_sizes:
        run: get_file_sizes.cwl
        in:
            file1: compile_1/object_file
            file2: compile_2/object_file
        out: [file_sizes]

requirements:
    MultipleInputFeatureRequirement: {}
