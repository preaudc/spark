/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.ml.param.shared

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param._

// DO NOT MODIFY THIS FILE! It was generated by SharedParamsCodeGen.

// scalastyle:off

/**
 * :: DeveloperApi ::
 * Trait for shared param regParam.
 */
@DeveloperApi
trait HasRegParam extends Params {

  /**
   * Param for regularization parameter.
   * @group param
   */
  final val regParam: DoubleParam = new DoubleParam(this, "regParam", "regularization parameter")

  /** @group getParam */
  final def getRegParam: Double = getOrDefault(regParam)
}

/**
 * :: DeveloperApi ::
 * Trait for shared param maxIter.
 */
@DeveloperApi
trait HasMaxIter extends Params {

  /**
   * Param for max number of iterations.
   * @group param
   */
  final val maxIter: IntParam = new IntParam(this, "maxIter", "max number of iterations")

  /** @group getParam */
  final def getMaxIter: Int = getOrDefault(maxIter)
}

/**
 * :: DeveloperApi ::
 * Trait for shared param featuresCol (default: "features").
 */
@DeveloperApi
trait HasFeaturesCol extends Params {

  /**
   * Param for features column name.
   * @group param
   */
  final val featuresCol: Param[String] = new Param[String](this, "featuresCol", "features column name")

  setDefault(featuresCol, "features")

  /** @group getParam */
  final def getFeaturesCol: String = getOrDefault(featuresCol)
}

/**
 * :: DeveloperApi ::
 * Trait for shared param labelCol (default: "label").
 */
@DeveloperApi
trait HasLabelCol extends Params {

  /**
   * Param for label column name.
   * @group param
   */
  final val labelCol: Param[String] = new Param[String](this, "labelCol", "label column name")

  setDefault(labelCol, "label")

  /** @group getParam */
  final def getLabelCol: String = getOrDefault(labelCol)
}

/**
 * :: DeveloperApi ::
 * Trait for shared param predictionCol (default: "prediction").
 */
@DeveloperApi
trait HasPredictionCol extends Params {

  /**
   * Param for prediction column name.
   * @group param
   */
  final val predictionCol: Param[String] = new Param[String](this, "predictionCol", "prediction column name")

  setDefault(predictionCol, "prediction")

  /** @group getParam */
  final def getPredictionCol: String = getOrDefault(predictionCol)
}

/**
 * :: DeveloperApi ::
 * Trait for shared param rawPredictionCol (default: "rawPrediction").
 */
@DeveloperApi
trait HasRawPredictionCol extends Params {

  /**
   * Param for raw prediction (a.k.a. confidence) column name.
   * @group param
   */
  final val rawPredictionCol: Param[String] = new Param[String](this, "rawPredictionCol", "raw prediction (a.k.a. confidence) column name")

  setDefault(rawPredictionCol, "rawPrediction")

  /** @group getParam */
  final def getRawPredictionCol: String = getOrDefault(rawPredictionCol)
}

/**
 * :: DeveloperApi ::
 * Trait for shared param probabilityCol (default: "probability").
 */
@DeveloperApi
trait HasProbabilityCol extends Params {

  /**
   * Param for column name for predicted class conditional probabilities.
   * @group param
   */
  final val probabilityCol: Param[String] = new Param[String](this, "probabilityCol", "column name for predicted class conditional probabilities")

  setDefault(probabilityCol, "probability")

  /** @group getParam */
  final def getProbabilityCol: String = getOrDefault(probabilityCol)
}

/**
 * :: DeveloperApi ::
 * Trait for shared param threshold.
 */
@DeveloperApi
trait HasThreshold extends Params {

  /**
   * Param for threshold in binary classification prediction.
   * @group param
   */
  final val threshold: DoubleParam = new DoubleParam(this, "threshold", "threshold in binary classification prediction")

  /** @group getParam */
  final def getThreshold: Double = getOrDefault(threshold)
}

/**
 * :: DeveloperApi ::
 * Trait for shared param inputCol.
 */
@DeveloperApi
trait HasInputCol extends Params {

  /**
   * Param for input column name.
   * @group param
   */
  final val inputCol: Param[String] = new Param[String](this, "inputCol", "input column name")

  /** @group getParam */
  final def getInputCol: String = getOrDefault(inputCol)
}

/**
 * :: DeveloperApi ::
 * Trait for shared param inputCols.
 */
@DeveloperApi
trait HasInputCols extends Params {

  /**
   * Param for input column names.
   * @group param
   */
  final val inputCols: Param[Array[String]] = new Param[Array[String]](this, "inputCols", "input column names")

  /** @group getParam */
  final def getInputCols: Array[String] = getOrDefault(inputCols)
}

/**
 * :: DeveloperApi ::
 * Trait for shared param outputCol.
 */
@DeveloperApi
trait HasOutputCol extends Params {

  /**
   * Param for output column name.
   * @group param
   */
  final val outputCol: Param[String] = new Param[String](this, "outputCol", "output column name")

  /** @group getParam */
  final def getOutputCol: String = getOrDefault(outputCol)
}

/**
 * :: DeveloperApi ::
 * Trait for shared param checkpointInterval.
 */
@DeveloperApi
trait HasCheckpointInterval extends Params {

  /**
   * Param for checkpoint interval.
   * @group param
   */
  final val checkpointInterval: IntParam = new IntParam(this, "checkpointInterval", "checkpoint interval")

  /** @group getParam */
  final def getCheckpointInterval: Int = getOrDefault(checkpointInterval)
}

/**
 * :: DeveloperApi ::
 * Trait for shared param fitIntercept (default: true).
 */
@DeveloperApi
trait HasFitIntercept extends Params {

  /**
   * Param for whether to fit an intercept term.
   * @group param
   */
  final val fitIntercept: BooleanParam = new BooleanParam(this, "fitIntercept", "whether to fit an intercept term")

  setDefault(fitIntercept, true)

  /** @group getParam */
  final def getFitIntercept: Boolean = getOrDefault(fitIntercept)
}
// scalastyle:on
