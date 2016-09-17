#Credit:  Bill Howe at University of Washington
#Note that this assumes the file is in flat text format
#
# Modified October 2014 by tmh to run under Python 3
#

# Modified October 2015 from original version to process text files instead of Json files
#


import json

class MapReduce:
    def __init__(self):
        self.intermediate = {}
        self.result = []

    def emit_intermediate(self, key, value):
        self.intermediate.setdefault(key, [])
        self.intermediate[key].append(value)

    def emit(self, value):
        self.result.append(value) 


    # pass execute an open file in 'data'
    def execute(self, data, mapper, reducer):

        inputFile = open (data);
        for eachLine in inputFile:
            mapper(eachLine)

        for key in self.intermediate:
            reducer(key, self.intermediate[key])

        print (self.result)

        for item in self.result:
            print (item)
