#
# Copyright 2015 Geoff MacGill
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

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from __future__ import with_statement

from ..base import ClientFrame


class Subscribe(ClientFrame):
    abstract = True

    @classmethod
    def random_id(cls):
        import os
        return os.urandom(16).encode('hex')


class Subscribe10(Subscribe):
    version = '1.0'
    verb = 'SUBSCRIBE'


class Subscribe11(Subscribe10):
    # pylint: disable=no-init
    version = '1.1'


class Subscribe12(Subscribe11):
    # pylint: disable=no-init
    version = '1.2'
