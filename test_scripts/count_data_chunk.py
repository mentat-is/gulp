#!/usr/bin/env python3
import json
import sys

# get args (min 1)
if len(sys.argv) < 2:
    print("Usage: count_data_chunk.py <path_to_json>")
    sys.exit(1)

json_path = sys.argv[1]
with open(json_path) as f:
    data = f.read()
    js = json.loads(data)
    print("number of docs in chunk: %d" % len(js["data"]["docs"]))
