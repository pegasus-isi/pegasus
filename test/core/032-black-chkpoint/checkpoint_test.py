#!/usr/bin/env python3

import optparse
import os
import shutil
import sys

checkpoint_file = "test.checkpoint"
max_value = 4  # the max value on which the script exits with status 0


def main():
    """
    A test executable for testing the job checkpointing support
    in Pegasus Planner.
    """

    # Configure command line option parser
    usage = "%s [options]" % sys.argv[0]
    description = "%s [o]" % sys.argv[0]

    parser = optparse.OptionParser(usage=usage, description=description)

    parser.add_option(
        "-o",
        "--output",
        action="append",
        type="str",
        dest="output_files",
        help="output file",
    )

    # Parsing command-line options
    (options, args) = parser.parse_args()

    if options.output_files is None:
        parser.error("Specify the -o option to specify the output file ")

    last_value = 0
    if os.path.isfile(checkpoint_file):
        print("checkpoint  file found %s" % checkpoint_file)
        print("reading checkpoint file %s" % checkpoint_file)

        # read in the last value from the checkpoint file
        with open(checkpoint_file) as CHECKPOINT_FILE:
            for my_line in CHECKPOINT_FILE:
                # print my_line
                contents = my_line.split()
                last_value = int(contents[0])

    # increment the value and append the updated value
    last_value = last_value + 1
    with open(checkpoint_file, "a") as CHECKPOINT_FILE:
        CHECKPOINT_FILE.write("%d \n" % last_value)
        print(f"Written value {last_value} to file {checkpoint_file} ")

    if last_value == max_value:
        # rename the test.checkpoint file to the output file
        for file in options.output_files:
            print(f"Copying checkpoint file {checkpoint_file} to output file {file} ")
            shutil.copy2(checkpoint_file, file)

        # delete the checkpoint file
        print("Deleting checkpoint file %s " % checkpoint_file)
        os.remove(checkpoint_file)
        sys.exit(0)
    else:
        sys.exit(1)


if __name__ == "__main__":
    main()
