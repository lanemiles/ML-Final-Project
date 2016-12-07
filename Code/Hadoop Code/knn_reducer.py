#!/usr/bin/env python

# import modules
import sys



def read_mapper_output(file, separator='\t'):
    """
    Create a generator for the results of reading from
    STDIN.
    """

    # Go through each line
    for line in file:
        yield line


def main(separator='\t'):
    """
    The actual reducer!

    In our case, this is a streaming version of the No-Op
    Reducer. It simply prints out what it receives from the
    Mapper.
    """

    # Read the data using read_mapper_output
    data = read_mapper_output(sys.stdin, separator=separator)

    # for each result, print it as we got it
    for result in data:

        # Write to stdout
        print result


if __name__ == "__main__":
    main()