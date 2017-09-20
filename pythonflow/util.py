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


class lazy_import:  # pylint: disable=invalid-name
    """
    Lazily import the given module.

    Parameters
    ----------
    module : str
        name of the module to import
    """
    def __init__(self, module):
        self.module = module
        self._module = None

    def __getattr__(self, name):
        if self._module is None:
            self._module = __import__(self.module)
        return getattr(self._module, name)
