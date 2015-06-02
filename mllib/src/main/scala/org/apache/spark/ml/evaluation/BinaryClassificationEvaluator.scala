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

package org.apache.spark.ml.evaluation

<<<<<<< HEAD
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml.Evaluator
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.SchemaUtils
=======
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.{Identifiable, SchemaUtils}
>>>>>>> upstream/master
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.DoubleType

/**
<<<<<<< HEAD
 * :: AlphaComponent ::
 *
 * Evaluator for binary classification, which expects two input columns: score and label.
 */
@AlphaComponent
class BinaryClassificationEvaluator extends Evaluator with Params
  with HasRawPredictionCol with HasLabelCol {
=======
 * :: Experimental ::
 * Evaluator for binary classification, which expects two input columns: score and label.
 */
@Experimental
class BinaryClassificationEvaluator(override val uid: String)
  extends Evaluator with HasRawPredictionCol with HasLabelCol {

  def this() = this(Identifiable.randomUID("binEval"))
>>>>>>> upstream/master

  /**
   * param for metric name in evaluation
   * @group param
   */
  val metricName: Param[String] = new Param(this, "metricName",
    "metric name in evaluation (areaUnderROC|areaUnderPR)")

  /** @group getParam */
<<<<<<< HEAD
  def getMetricName: String = getOrDefault(metricName)
=======
  def getMetricName: String = $(metricName)
>>>>>>> upstream/master

  /** @group setParam */
  def setMetricName(value: String): this.type = set(metricName, value)

  /** @group setParam */
  def setScoreCol(value: String): this.type = set(rawPredictionCol, value)

  /** @group setParam */
  def setLabelCol(value: String): this.type = set(labelCol, value)

  setDefault(metricName -> "areaUnderROC")

<<<<<<< HEAD
  override def evaluate(dataset: DataFrame, paramMap: ParamMap): Double = {
    val map = extractParamMap(paramMap)

    val schema = dataset.schema
    SchemaUtils.checkColumnType(schema, map(rawPredictionCol), new VectorUDT)
    SchemaUtils.checkColumnType(schema, map(labelCol), DoubleType)

    // TODO: When dataset metadata has been implemented, check rawPredictionCol vector length = 2.
    val scoreAndLabels = dataset.select(map(rawPredictionCol), map(labelCol))
=======
  override def evaluate(dataset: DataFrame): Double = {
    val schema = dataset.schema
    SchemaUtils.checkColumnType(schema, $(rawPredictionCol), new VectorUDT)
    SchemaUtils.checkColumnType(schema, $(labelCol), DoubleType)

    // TODO: When dataset metadata has been implemented, check rawPredictionCol vector length = 2.
    val scoreAndLabels = dataset.select($(rawPredictionCol), $(labelCol))
>>>>>>> upstream/master
      .map { case Row(rawPrediction: Vector, label: Double) =>
        (rawPrediction(1), label)
      }
    val metrics = new BinaryClassificationMetrics(scoreAndLabels)
<<<<<<< HEAD
    val metric = map(metricName) match {
=======
    val metric = $(metricName) match {
>>>>>>> upstream/master
      case "areaUnderROC" =>
        metrics.areaUnderROC()
      case "areaUnderPR" =>
        metrics.areaUnderPR()
      case other =>
        throw new IllegalArgumentException(s"Does not support metric $other.")
    }
    metrics.unpersist()
    metric
  }
}
