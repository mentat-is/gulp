#!/usr/bin/env python3
"""
count the number of events in one or more evtx files
"""
import json
import os
import sys

import muty.file
from evtx import PyEvtxParser

count = 0
failed = 0


def process_file(file):
    global count, failed
    print("parsing %s ..." % (file))

    parser = PyEvtxParser(file, number_of_threads=8)
    try:
        it = parser.records()
        for r in it:
            count += 1
    except:
        failed += 1


def process_files(files):
    for f in files:
        process_file(f)


def main():
    # first parameter is the path to the evtx file
    if len(sys.argv) < 2:
        print("count the number of events in one or more evtx files")
        print("Usage: evtx_count.py <evtx_file|directory>")
        return

    filepath = sys.argv[1]
    if os.path.isfile(filepath):
        process_file(filepath)
    else:
        files = muty.file.list_directory(os.path.abspath(filepath))
        process_files(files)
    print("total events count: processed=%d, failed=%d" % (count, failed))


if __name__ == "__main__":
    main()
