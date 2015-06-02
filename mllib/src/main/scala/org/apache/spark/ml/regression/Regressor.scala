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

package org.apache.spark.ml.regression

<<<<<<< HEAD
import org.apache.spark.annotation.{DeveloperApi, AlphaComponent}
import org.apache.spark.ml.impl.estimator.{PredictionModel, Predictor, PredictorParams}

/**
 * :: DeveloperApi ::
 * Params for regression.
 * Currently empty, but may add functionality later.
 *
 * NOTE: This is currently private[spark] but will be made public later once it is stabilized.
 */
@DeveloperApi
private[spark] trait RegressorParams extends PredictorParams

/**
 * :: AlphaComponent ::
=======
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.{PredictionModel, PredictorParams, Predictor}


/**
 * :: DeveloperApi ::
>>>>>>> upstream/master
 *
 * Single-label regression
 *
 * @tparam FeaturesType  Type of input features.  E.g., [[org.apache.spark.mllib.linalg.Vector]]
 * @tparam Learner  Concrete Estimator type
 * @tparam M  Concrete Model type
<<<<<<< HEAD
 *
 * NOTE: This is currently private[spark] but will be made public later once it is stabilized.
 */
@AlphaComponent
=======
 */
@DeveloperApi
>>>>>>> upstream/master
private[spark] abstract class Regressor[
    FeaturesType,
    Learner <: Regressor[FeaturesType, Learner, M],
    M <: RegressionModel[FeaturesType, M]]
<<<<<<< HEAD
  extends Predictor[FeaturesType, Learner, M]
  with RegressorParams {
=======
  extends Predictor[FeaturesType, Learner, M] with PredictorParams {
>>>>>>> upstream/master

  // TODO: defaultEvaluator (follow-up PR)
}

/**
<<<<<<< HEAD
 * :: AlphaComponent ::
=======
 * :: DeveloperApi ::
>>>>>>> upstream/master
 *
 * Model produced by a [[Regressor]].
 *
 * @tparam FeaturesType  Type of input features.  E.g., [[org.apache.spark.mllib.linalg.Vector]]
 * @tparam M  Concrete Model type.
<<<<<<< HEAD
 *
 * NOTE: This is currently private[spark] but will be made public later once it is stabilized.
 */
@AlphaComponent
private[spark] abstract class RegressionModel[FeaturesType, M <: RegressionModel[FeaturesType, M]]
  extends PredictionModel[FeaturesType, M] with RegressorParams {

  /**
   * :: DeveloperApi ::
   *
   * Predict real-valued label for the given features.
   * This internal method is used to implement [[transform()]] and output [[predictionCol]].
   */
  @DeveloperApi
  protected def predict(features: FeaturesType): Double

=======
 */
@DeveloperApi
abstract class RegressionModel[FeaturesType, M <: RegressionModel[FeaturesType, M]]
  extends PredictionModel[FeaturesType, M] with PredictorParams {

  // TODO: defaultEvaluator (follow-up PR)
>>>>>>> upstream/master
}
