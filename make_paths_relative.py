#!/usr/bin/env python
import os
import re
import sys

pattern = re.compile(r"\s*-e\s+file://(?P<path>.*?)\n")
inputs = sys.stdin.readlines()
for line in inputs:
    match = pattern.match(line)
    if match:
        path = match.group("path")
        path = os.path.relpath(path)
        line = f"-e {path}\n"
    sys.stdout.write(line)
