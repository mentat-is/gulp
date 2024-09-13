#!/usr/bin/env python3
import json
import os

pwd = os.path.dirname(__file__)
path = muty.file.safe_path_join(pwd, "./test.json")
with open(path, "r") as file:
    data = json.load(file)

num_elements = len(data["data"]["category"]["buckets"][0]["histograms"]["buckets"])
print(num_elements)
