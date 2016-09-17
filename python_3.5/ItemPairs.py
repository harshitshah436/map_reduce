"""
ItemPairs.py

Programming Project: Determine all item pairs occurring more than 650 times in order to appear on an endcap.

Team Members: Amit Doshi <ad5418@rit.edu>, Harshit Shah <hrs8207@rit.edu>
"""

import sys
import MapReduce

__authors__ = "Amit Doshi, Harshit Shah"
__version__ = "1.0"

# Instantiate MapReduce class.
mr = MapReduce.MapReduce()


def get_items(data):
    """
    Create a list of all the items from the given input line.

    :param data: a record/line from a data file
    :return a list containing all the items from a given line
    """

    # Remove ']' from the given record/line.
    data = data.replace(']', '')

    # Split the record and get a list of items.
    tokens = data.split(',')
    tokens = tokens[1].split()

    return tokens


def get_pair(item1, item2):
    """
    Create an item-pair from given two items in a lexical order.

    :param item1: first item
    :param item2: second item
    :return an item-pair
    """
    return (item1 + '/' + item2) if item1 < item2 else (item2 + '/' + item1)


def mapper(data):
    """
    Mapper function to process each line.

    :param data: line/record from an input file
    :return None
    """

    # Get a list of items.
    tokens = get_items(data)

    # Intermediate emit each distinct item-pair with count 1.
    for index in range(len(tokens)):
        for index1 in range(index + 1, len(tokens)):
            item_pair = get_pair(tokens[index], tokens[index1])
            mr.emit_intermediate(item_pair, 1)


def reducer(key, list_of_values):
    """
    Reducer function to reduce key and all the values associated with the key emitted from Mapper.

    :param key: URL
    :param list_of_values: 1 in a list for each time URL encountered
    :return None
    """

    # Check if an item-pair is with count greater than 650. If so then emit it from the reducer.
    if len(list_of_values) > 650:
        mr.emit(key)


if __name__ == '__main__':

    # If more then one command-line argument specified, print usage message and exit.
    if len(sys.argv) != 2:
        print("Expecting one argument.\nUsage: ItemPairs.py <input_file>")
        sys.exit(1)

    # Handle file not found error.
    try:
        # Call execute function from MapReduce module.
        mr.execute(sys.argv[1], mapper, reducer)
    except FileNotFoundError:
        print("Input file not found. Please enter correct file name.")
        sys.exit(2)
