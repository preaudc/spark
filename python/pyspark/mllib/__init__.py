#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Python bindings for MLlib.
"""
from __future__ import absolute_import

# MLlib currently needs NumPy 1.4+, so complain if lower

import numpy

ver = [int(x) for x in numpy.version.version.split('.')[:2]]
if ver < [1, 4]:
    raise Exception("MLlib requires NumPy 1.4+")

__all__ = ['classification', 'clustering', 'feature', 'fpm', 'linalg', 'random',
           'recommendation', 'regression', 'stat', 'tree', 'util']
<<<<<<< HEAD

import sys
from . import rand as random
modname = __name__ + '.random'
random.__name__ = modname
random.RandomRDDs.__module__ = modname
sys.modules[modname] = random
del modname, sys
=======
>>>>>>> upstream/master
