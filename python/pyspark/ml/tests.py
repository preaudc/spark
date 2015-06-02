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
Unit tests for Spark ML Python APIs.
"""

import sys

if sys.version_info[:2] <= (2, 6):
    try:
        import unittest2 as unittest
    except ImportError:
        sys.stderr.write('Please install unittest2 to test with Python 2.6 or earlier')
        sys.exit(1)
else:
    import unittest

from pyspark.tests import ReusedPySparkTestCase as PySparkTestCase
<<<<<<< HEAD
from pyspark.sql import DataFrame
from pyspark.ml.param import Param
from pyspark.ml.param.shared import HasMaxIter, HasInputCol
from pyspark.ml.pipeline import Transformer, Estimator, Pipeline
=======
from pyspark.sql import DataFrame, SQLContext
from pyspark.ml.param import Param, Params
from pyspark.ml.param.shared import HasMaxIter, HasInputCol, HasSeed
from pyspark.ml.util import keyword_only
from pyspark.ml import Estimator, Model, Pipeline, Transformer
from pyspark.ml.feature import *
from pyspark.mllib.linalg import DenseVector
>>>>>>> upstream/master


class MockDataset(DataFrame):

    def __init__(self):
        self.index = 0


<<<<<<< HEAD
class MockTransformer(Transformer):

    def __init__(self):
        super(MockTransformer, self).__init__()
        self.fake = Param(self, "fake", "fake")
        self.dataset_index = None
        self.fake_param_value = None

    def transform(self, dataset, params={}):
        self.dataset_index = dataset.index
        if self.fake in params:
            self.fake_param_value = params[self.fake]
=======
class HasFake(Params):

    def __init__(self):
        super(HasFake, self).__init__()
        self.fake = Param(self, "fake", "fake param")

    def getFake(self):
        return self.getOrDefault(self.fake)


class MockTransformer(Transformer, HasFake):

    def __init__(self):
        super(MockTransformer, self).__init__()
        self.dataset_index = None

    def _transform(self, dataset):
        self.dataset_index = dataset.index
>>>>>>> upstream/master
        dataset.index += 1
        return dataset


<<<<<<< HEAD
class MockEstimator(Estimator):

    def __init__(self):
        super(MockEstimator, self).__init__()
        self.fake = Param(self, "fake", "fake")
        self.dataset_index = None
        self.fake_param_value = None
        self.model = None

    def fit(self, dataset, params={}):
        self.dataset_index = dataset.index
        if self.fake in params:
            self.fake_param_value = params[self.fake]
        model = MockModel()
        self.model = model
        return model


class MockModel(MockTransformer, Transformer):

    def __init__(self):
        super(MockModel, self).__init__()
=======
class MockEstimator(Estimator, HasFake):

    def __init__(self):
        super(MockEstimator, self).__init__()
        self.dataset_index = None

    def _fit(self, dataset):
        self.dataset_index = dataset.index
        model = MockModel()
        self._copyValues(model)
        return model


class MockModel(MockTransformer, Model, HasFake):
    pass
>>>>>>> upstream/master


class PipelineTests(PySparkTestCase):

    def test_pipeline(self):
        dataset = MockDataset()
        estimator0 = MockEstimator()
        transformer1 = MockTransformer()
        estimator2 = MockEstimator()
        transformer3 = MockTransformer()
<<<<<<< HEAD
        pipeline = Pipeline() \
            .setStages([estimator0, transformer1, estimator2, transformer3])
        pipeline_model = pipeline.fit(dataset, {estimator0.fake: 0, transformer1.fake: 1})
        self.assertEqual(0, estimator0.dataset_index)
        self.assertEqual(0, estimator0.fake_param_value)
        model0 = estimator0.model
        self.assertEqual(0, model0.dataset_index)
        self.assertEqual(1, transformer1.dataset_index)
        self.assertEqual(1, transformer1.fake_param_value)
        self.assertEqual(2, estimator2.dataset_index)
        model2 = estimator2.model
        self.assertIsNone(model2.dataset_index, "The model produced by the last estimator should "
                                                "not be called during fit.")
=======
        pipeline = Pipeline(stages=[estimator0, transformer1, estimator2, transformer3])
        pipeline_model = pipeline.fit(dataset, {estimator0.fake: 0, transformer1.fake: 1})
        model0, transformer1, model2, transformer3 = pipeline_model.stages
        self.assertEqual(0, model0.dataset_index)
        self.assertEqual(0, model0.getFake())
        self.assertEqual(1, transformer1.dataset_index)
        self.assertEqual(1, transformer1.getFake())
        self.assertEqual(2, dataset.index)
        self.assertIsNone(model2.dataset_index, "The last model shouldn't be called in fit.")
        self.assertIsNone(transformer3.dataset_index,
                          "The last transformer shouldn't be called in fit.")
>>>>>>> upstream/master
        dataset = pipeline_model.transform(dataset)
        self.assertEqual(2, model0.dataset_index)
        self.assertEqual(3, transformer1.dataset_index)
        self.assertEqual(4, model2.dataset_index)
        self.assertEqual(5, transformer3.dataset_index)
        self.assertEqual(6, dataset.index)


<<<<<<< HEAD
class TestParams(HasMaxIter, HasInputCol):
    """
    A subclass of Params mixed with HasMaxIter and HasInputCol.
    """

    def __init__(self):
        super(TestParams, self).__init__()
        self._setDefault(maxIter=10)
=======
class TestParams(HasMaxIter, HasInputCol, HasSeed):
    """
    A subclass of Params mixed with HasMaxIter, HasInputCol and HasSeed.
    """
    @keyword_only
    def __init__(self, seed=None):
        super(TestParams, self).__init__()
        self._setDefault(maxIter=10)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, seed=None):
        """
        setParams(self, seed=None)
        Sets params for this test.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)


class OtherTestParams(HasMaxIter, HasInputCol, HasSeed):
    """
    A subclass of Params mixed with HasMaxIter, HasInputCol and HasSeed.
    """
    @keyword_only
    def __init__(self, seed=None):
        super(OtherTestParams, self).__init__()
        self._setDefault(maxIter=10)
        kwargs = self.__init__._input_kwargs
        self.setParams(**kwargs)

    @keyword_only
    def setParams(self, seed=None):
        """
        setParams(self, seed=None)
        Sets params for this test.
        """
        kwargs = self.setParams._input_kwargs
        return self._set(**kwargs)
>>>>>>> upstream/master


class ParamTests(PySparkTestCase):

    def test_param(self):
        testParams = TestParams()
        maxIter = testParams.maxIter
        self.assertEqual(maxIter.name, "maxIter")
<<<<<<< HEAD
        self.assertEqual(maxIter.doc, "max number of iterations")
        self.assertTrue(maxIter.parent is testParams)
=======
        self.assertEqual(maxIter.doc, "max number of iterations (>= 0)")
        self.assertTrue(maxIter.parent == testParams.uid)
>>>>>>> upstream/master

    def test_params(self):
        testParams = TestParams()
        maxIter = testParams.maxIter
        inputCol = testParams.inputCol
<<<<<<< HEAD

        params = testParams.params
        self.assertEqual(params, [inputCol, maxIter])

=======
        seed = testParams.seed

        params = testParams.params
        self.assertEqual(params, [inputCol, maxIter, seed])

        self.assertTrue(testParams.hasParam(maxIter))
>>>>>>> upstream/master
        self.assertTrue(testParams.hasDefault(maxIter))
        self.assertFalse(testParams.isSet(maxIter))
        self.assertTrue(testParams.isDefined(maxIter))
        self.assertEqual(testParams.getMaxIter(), 10)
        testParams.setMaxIter(100)
        self.assertTrue(testParams.isSet(maxIter))
        self.assertEquals(testParams.getMaxIter(), 100)

<<<<<<< HEAD
=======
        self.assertTrue(testParams.hasParam(inputCol))
>>>>>>> upstream/master
        self.assertFalse(testParams.hasDefault(inputCol))
        self.assertFalse(testParams.isSet(inputCol))
        self.assertFalse(testParams.isDefined(inputCol))
        with self.assertRaises(KeyError):
            testParams.getInputCol()

<<<<<<< HEAD
        self.assertEquals(
            testParams.explainParams(),
            "\n".join(["inputCol: input column name (undefined)",
                       "maxIter: max number of iterations (default: 10, current: 100)"]))
=======
        # Since the default is normally random, set it to a known number for debug str
        testParams._setDefault(seed=41)
        testParams.setSeed(43)

        self.assertEquals(
            testParams.explainParams(),
            "\n".join(["inputCol: input column name (undefined)",
                       "maxIter: max number of iterations (>= 0) (default: 10, current: 100)",
                       "seed: random seed (default: 41, current: 43)"]))

    def test_hasseed(self):
        noSeedSpecd = TestParams()
        withSeedSpecd = TestParams(seed=42)
        other = OtherTestParams()
        # Check that we no longer use 42 as the magic number
        self.assertNotEqual(noSeedSpecd.getSeed(), 42)
        origSeed = noSeedSpecd.getSeed()
        # Check that we only compute the seed once
        self.assertEqual(noSeedSpecd.getSeed(), origSeed)
        # Check that a specified seed is honored
        self.assertEqual(withSeedSpecd.getSeed(), 42)
        # Check that a different class has a different seed
        self.assertNotEqual(other.getSeed(), noSeedSpecd.getSeed())


class FeatureTests(PySparkTestCase):

    def test_binarizer(self):
        b0 = Binarizer()
        self.assertListEqual(b0.params, [b0.inputCol, b0.outputCol, b0.threshold])
        self.assertTrue(all([~b0.isSet(p) for p in b0.params]))
        self.assertTrue(b0.hasDefault(b0.threshold))
        self.assertEqual(b0.getThreshold(), 0.0)
        b0.setParams(inputCol="input", outputCol="output").setThreshold(1.0)
        self.assertTrue(all([b0.isSet(p) for p in b0.params]))
        self.assertEqual(b0.getThreshold(), 1.0)
        self.assertEqual(b0.getInputCol(), "input")
        self.assertEqual(b0.getOutputCol(), "output")

        b0c = b0.copy({b0.threshold: 2.0})
        self.assertEqual(b0c.uid, b0.uid)
        self.assertListEqual(b0c.params, b0.params)
        self.assertEqual(b0c.getThreshold(), 2.0)

        b1 = Binarizer(threshold=2.0, inputCol="input", outputCol="output")
        self.assertNotEqual(b1.uid, b0.uid)
        self.assertEqual(b1.getThreshold(), 2.0)
        self.assertEqual(b1.getInputCol(), "input")
        self.assertEqual(b1.getOutputCol(), "output")

    def test_idf(self):
        sqlContext = SQLContext(self.sc)
        dataset = sqlContext.createDataFrame([
            (DenseVector([1.0, 2.0]),),
            (DenseVector([0.0, 1.0]),),
            (DenseVector([3.0, 0.2]),)], ["tf"])
        idf0 = IDF(inputCol="tf")
        self.assertListEqual(idf0.params, [idf0.inputCol, idf0.minDocFreq, idf0.outputCol])
        idf0m = idf0.fit(dataset, {idf0.outputCol: "idf"})
        self.assertEqual(idf0m.uid, idf0.uid,
                         "Model should inherit the UID from its parent estimator.")
        output = idf0m.transform(dataset)
        self.assertIsNotNone(output.head().idf)
>>>>>>> upstream/master


if __name__ == "__main__":
    unittest.main()
