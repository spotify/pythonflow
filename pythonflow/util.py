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

import math


class lazy_import:  # pylint: disable=invalid-name, too-few-public-methods
    """
    Lazily import the given module.

    Parameters
    ----------
    module : str
        Name of the module to import
    """
    def __init__(self, module):
        self.module = module
        self._module = None

    def __getattr__(self, name):
        if self._module is None:
            self._module = __import__(self.module)
        return getattr(self._module, name)

class batch_iterable:  # pylint: disable=invalid-name, too-few-public-methods
    """
    Split an iterable into batches of a specified size.

    Parameters
    ----------
    iterable : iterable
        Iterable to split into batches.
    batch_size : int
        Size of each batch.
    transpose : bool
        Whether to transpose each batch.
    """
    def __init__(self, iterable, batch_size, transpose=False):
        self.iterable = iterable
        if batch_size <= 0:
            raise ValueError("`batch_size` must be positive but got '%s'" % batch_size)
        self.batch_size = batch_size
        self.transpose = transpose

    def __len__(self):
        return math.ceil(len(self.iterable) / self.batch_size)

    def __iter__(self):
        batch = []
        for item in self.iterable:
            batch.append(item)
            if len(batch) == self.batch_size:
                yield zip(*batch) if self.transpose else batch
                batch = []
        if batch:
            yield zip(*batch) if self.transpose else batch
