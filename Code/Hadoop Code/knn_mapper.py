#!/usr/bin/env python

# Use the sys module
import sys
import math
import os


def parse_str_to_map(inp):
    """
    Given the sparse representation of an example as a string,
    parse it into a HashMap.

    "{1=2, 3=4}" --> HashMap w. {1 -> 2, 3 -> 4}
    """

    feature_map = {}

    kv_pairs = inp.split(",")
    kv_pairs[0] = kv_pairs[0][1:]
    kv_pairs[-1] = kv_pairs[-1][:-1]

    for kv in kv_pairs:
        pieces = kv.split("=")
        key = pieces[0].strip()
        value = pieces[1].strip()
        feature_map[key] = int(value)

    return feature_map


def parse_string_to_labeled_example(inp):
    """
    Given an example with a tab separated label,
    split it, get the feature map, and return both.
    """

    pieces = inp.split("\t")
    label = pieces[0]
    feature_map = parse_str_to_map(pieces[1])
    return (label, feature_map)


def parse_string_to_unlabeled_example(inp):
    """
    Given an example without a label, get and
    return the feature map
    """

    return parse_str_to_map(inp)


# 'file' in this case is STDIN
def read_input(file):
    """
    Return a generator that iterates over the
    input given to the mapper.
    """

    for line in file:
        yield line


def main(separator='\t'):
    """
    The actual mapper!

    Unlike Java Hadoop where we read from a file, in the streaming
    version of Hadoop we read from STDIN and write to STDOUT.
    """

    # get the test example
    test_example_str = os.environ["EXAMPLE_STR"].replace("_", " ")

    # parse the test example into a feature map
    test_ex_feature_map = parse_string_to_unlabeled_example(test_example_str)

    # for each training example on this mapper
    input_lines = read_input(sys.stdin)
    for line in input_lines:

        # remove final "\n"
        line = line[:-1]

        # parse the example into its label and feature map
        (label, f_map) = parse_string_to_labeled_example(line)

        # compute the cosine similarity, then 1 - it for sorting purposes
        cosine_sim = float(1 - float(cosine_similarity(test_ex_feature_map, f_map)))

        # output to reduce phase
        print '%f%s%s' % (cosine_sim, separator, label)


def cosine_similarity(test_ex_map, train_ex_map):
    """
    For two feature maps, compute their cosine similarity.
    CS(a, b) = dot_prod(a, b) / (norm(a) * norm(b))
    """

    # compute denominator
    cosine_numerator = 0
    train_sum = 0

    for (feature_idx, feature_count) in train_ex_map.items():
        if feature_idx in test_ex_map:
            cosine_numerator += (test_ex_map[feature_idx] * train_ex_map[feature_idx])
        train_sum += (feature_count * feature_count)

    test_sum = 0
    for (feature_idx, feature_count) in test_ex_map.items():
        test_sum += (feature_count * feature_count)

    cosine_denominator = math.sqrt(train_sum) * math.sqrt(test_sum)
    return (cosine_numerator * 1.0) / cosine_denominator


if __name__ == "__main__":
    main()