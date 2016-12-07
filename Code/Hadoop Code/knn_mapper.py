#!/usr/bin/env python

# Use the sys module
import sys
import math
import os


def parse_str_to_map(inp):

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

    pieces = inp.split("\t")
    label = pieces[0]
    feature_map = parse_str_to_map(pieces[1])
    return (label, feature_map)


def parse_string_to_unlabeled_example(inp):

    return parse_str_to_map(inp)


# 'file' in this case is STDIN
def read_input(file):
    # Split each line into words
    for line in file:
        yield line


def main(separator='\t'):

    # print os.environ
    test_example_str = os.environ["EXAMPLE_STR"].replace("_", " ")

    # get the test example data
    test_ex_feature_map = parse_string_to_unlabeled_example(test_example_str)

    # Read the data using read_input
    input_lines = read_input(sys.stdin)

    for line in input_lines:

        line = line[:-1]

        (label, f_map) = parse_string_to_labeled_example(line)

        # compute the cosine similarity
        cosine_sim = float(1 - float(cosine_similarity(test_ex_feature_map, f_map)))

        print '%f%s%s' % (cosine_sim, separator, label)


def cosine_similarity(test_ex_map, train_ex_map):

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