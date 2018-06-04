# pylint: disable=missing-docstring
# pylint: enable=missing-docstring
# Copyright 2017 Spotify AB
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import sys
from setuptools import setup, find_packages

if sys.version_info[0] < 3:
    raise RuntimeError("pythonflow requires python3 but you are using %s" % sys.version)

with open('README.md') as fp:
    long_description = fp.read()

setup(
    name="pythonflow",
    description="Dataflow programming for python",
    version="0.2.0",
    author="Till Hoffmann",
    author_email="till@spotify.com",
    license="License :: OSI Approved :: Apache Software License",
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Programming Language :: Python :: 3",
        "Topic :: Scientific/Engineering :: Artificial Intelligence",
        "Topic :: Utilities"
    ],
    url="https://github.com/spotify/pythonflow",
    packages=find_packages(),
    long_description=long_description,
    long_description_content_type="text/markdown",
)
