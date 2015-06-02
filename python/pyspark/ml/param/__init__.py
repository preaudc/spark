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

from abc import ABCMeta
<<<<<<< HEAD
=======
import copy
>>>>>>> upstream/master

from pyspark.ml.util import Identifiable


__all__ = ['Param', 'Params']


class Param(object):
    """
    A param with self-contained documentation.
    """

    def __init__(self, parent, name, doc):
<<<<<<< HEAD
        if not isinstance(parent, Params):
            raise ValueError("Parent must be a Params but got type %s." % type(parent).__name__)
        self.parent = parent
=======
        if not isinstance(parent, Identifiable):
            raise TypeError("Parent must be an Identifiable but got type %s." % type(parent))
        self.parent = parent.uid
>>>>>>> upstream/master
        self.name = str(name)
        self.doc = str(doc)

    def __str__(self):
        return str(self.parent) + "__" + self.name

    def __repr__(self):
        return "Param(parent=%r, name=%r, doc=%r)" % (self.parent, self.name, self.doc)

<<<<<<< HEAD
=======
    def __hash__(self):
        return hash(str(self))

    def __eq__(self, other):
        if isinstance(other, Param):
            return self.parent == other.parent and self.name == other.name
        else:
            return False

>>>>>>> upstream/master

class Params(Identifiable):
    """
    Components that take parameters. This also provides an internal
    param map to store parameter values attached to the instance.
    """

    __metaclass__ = ABCMeta

    #: internal param map for user-supplied values param map
<<<<<<< HEAD
    paramMap = {}

    #: internal param map for default values
    defaultParamMap = {}
=======
    _paramMap = {}

    #: internal param map for default values
    _defaultParamMap = {}

    #: value returned by :py:func:`params`
    _params = None
>>>>>>> upstream/master

    @property
    def params(self):
        """
        Returns all params ordered by name. The default implementation
        uses :py:func:`dir` to get all attributes of type
        :py:class:`Param`.
        """
<<<<<<< HEAD
        return list(filter(lambda attr: isinstance(attr, Param),
                           [getattr(self, x) for x in dir(self) if x != "params"]))

    def _explain(self, param):
=======
        if self._params is None:
            self._params = list(filter(lambda attr: isinstance(attr, Param),
                                       [getattr(self, x) for x in dir(self) if x != "params"]))
        return self._params

    def explainParam(self, param):
>>>>>>> upstream/master
        """
        Explains a single param and returns its name, doc, and optional
        default value and user-supplied value in a string.
        """
        param = self._resolveParam(param)
        values = []
        if self.isDefined(param):
<<<<<<< HEAD
            if param in self.defaultParamMap:
                values.append("default: %s" % self.defaultParamMap[param])
            if param in self.paramMap:
                values.append("current: %s" % self.paramMap[param])
=======
            if param in self._defaultParamMap:
                values.append("default: %s" % self._defaultParamMap[param])
            if param in self._paramMap:
                values.append("current: %s" % self._paramMap[param])
>>>>>>> upstream/master
        else:
            values.append("undefined")
        valueStr = "(" + ", ".join(values) + ")"
        return "%s: %s %s" % (param.name, param.doc, valueStr)

    def explainParams(self):
        """
        Returns the documentation of all params with their optionally
        default values and user-supplied values.
        """
<<<<<<< HEAD
        return "\n".join([self._explain(param) for param in self.params])
=======
        return "\n".join([self.explainParam(param) for param in self.params])
>>>>>>> upstream/master

    def getParam(self, paramName):
        """
        Gets a param by its name.
        """
        param = getattr(self, paramName)
        if isinstance(param, Param):
            return param
        else:
            raise ValueError("Cannot find param with name %s." % paramName)

    def isSet(self, param):
        """
        Checks whether a param is explicitly set by user.
        """
        param = self._resolveParam(param)
<<<<<<< HEAD
        return param in self.paramMap
=======
        return param in self._paramMap
>>>>>>> upstream/master

    def hasDefault(self, param):
        """
        Checks whether a param has a default value.
        """
        param = self._resolveParam(param)
<<<<<<< HEAD
        return param in self.defaultParamMap

    def isDefined(self, param):
        """
        Checks whether a param is explicitly set by user or has a default value.
        """
        return self.isSet(param) or self.hasDefault(param)

    def getOrDefault(self, param):
        """
        Gets the value of a param in the user-supplied param map or its
        default value. Raises an error if either is set.
        """
        if isinstance(param, Param):
            if param in self.paramMap:
                return self.paramMap[param]
            else:
                return self.defaultParamMap[param]
        elif isinstance(param, str):
            return self.getOrDefault(self.getParam(param))
        else:
            raise KeyError("Cannot recognize %r as a param." % param)

    def extractParamMap(self, extraParamMap={}):
=======
        return param in self._defaultParamMap

    def isDefined(self, param):
        """
        Checks whether a param is explicitly set by user or has
        a default value.
        """
        return self.isSet(param) or self.hasDefault(param)

    def hasParam(self, paramName):
        """
        Tests whether this instance contains a param with a given
        (string) name.
        """
        param = self._resolveParam(paramName)
        return param in self.params

    def getOrDefault(self, param):
        """
        Gets the value of a param in the user-supplied param map or its
        default value. Raises an error if neither is set.
        """
        param = self._resolveParam(param)
        if param in self._paramMap:
            return self._paramMap[param]
        else:
            return self._defaultParamMap[param]

    def extractParamMap(self, extra={}):
>>>>>>> upstream/master
        """
        Extracts the embedded default param values and user-supplied
        values, and then merges them with extra values from input into
        a flat param map, where the latter value is used if there exist
        conflicts, i.e., with ordering: default param values <
<<<<<<< HEAD
        user-supplied values < extraParamMap.
        :param extraParamMap: extra param values
        :return: merged param map
        """
        paramMap = self.defaultParamMap.copy()
        paramMap.update(self.paramMap)
        paramMap.update(extraParamMap)
        return paramMap

=======
        user-supplied values < extra.
        :param extra: extra param values
        :return: merged param map
        """
        paramMap = self._defaultParamMap.copy()
        paramMap.update(self._paramMap)
        paramMap.update(extra)
        return paramMap

    def copy(self, extra={}):
        """
        Creates a copy of this instance with the same uid and some
        extra params. The default implementation creates a
        shallow copy using :py:func:`copy.copy`, and then copies the
        embedded and extra parameters over and returns the copy.
        Subclasses should override this method if the default approach
        is not sufficient.
        :param extra: Extra parameters to copy to the new instance
        :return: Copy of this instance
        """
        that = copy.copy(self)
        that._paramMap = self.extractParamMap(extra)
        return that

>>>>>>> upstream/master
    def _shouldOwn(self, param):
        """
        Validates that the input param belongs to this Params instance.
        """
<<<<<<< HEAD
        if param.parent is not self:
=======
        if not (self.uid == param.parent and self.hasParam(param.name)):
>>>>>>> upstream/master
            raise ValueError("Param %r does not belong to %r." % (param, self))

    def _resolveParam(self, param):
        """
        Resolves a param and validates the ownership.
        :param param: param name or the param instance, which must
                      belong to this Params instance
        :return: resolved param instance
        """
        if isinstance(param, Param):
            self._shouldOwn(param)
            return param
        elif isinstance(param, str):
            return self.getParam(param)
        else:
            raise ValueError("Cannot resolve %r as a param." % param)

    @staticmethod
    def _dummy():
        """
<<<<<<< HEAD
        Returns a dummy Params instance used as a placeholder to generate docs.
=======
        Returns a dummy Params instance used as a placeholder to
        generate docs.
>>>>>>> upstream/master
        """
        dummy = Params()
        dummy.uid = "undefined"
        return dummy

    def _set(self, **kwargs):
        """
        Sets user-supplied params.
        """
        for param, value in kwargs.items():
<<<<<<< HEAD
            self.paramMap[getattr(self, param)] = value
=======
            self._paramMap[getattr(self, param)] = value
>>>>>>> upstream/master
        return self

    def _setDefault(self, **kwargs):
        """
        Sets default params.
        """
        for param, value in kwargs.items():
<<<<<<< HEAD
            self.defaultParamMap[getattr(self, param)] = value
        return self
=======
            self._defaultParamMap[getattr(self, param)] = value
        return self

    def _copyValues(self, to, extra={}):
        """
        Copies param values from this instance to another instance for
        params shared by them.
        :param to: the target instance
        :param extra: extra params to be copied
        :return: the target instance with param values copied
        """
        paramMap = self.extractParamMap(extra)
        for p in self.params:
            if p in paramMap and to.hasParam(p.name):
                to._set(**{p.name: paramMap[p]})
        return to
>>>>>>> upstream/master
