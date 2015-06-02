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

package org.apache.spark.ml.feature

<<<<<<< HEAD
import org.apache.spark.annotation.AlphaComponent
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
=======
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml._
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.shared._
import org.apache.spark.ml.util.Identifiable
>>>>>>> upstream/master
import org.apache.spark.mllib.feature
import org.apache.spark.mllib.linalg.{Vector, VectorUDT}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * Params for [[StandardScaler]] and [[StandardScalerModel]].
 */
<<<<<<< HEAD
private[feature] trait StandardScalerParams extends Params with HasInputCol with HasOutputCol

/**
 * :: AlphaComponent ::
 * Standardizes features by removing the mean and scaling to unit variance using column summary
 * statistics on the samples in the training set.
 */
@AlphaComponent
class StandardScaler extends Estimator[StandardScalerModel] with StandardScalerParams {
=======
private[feature] trait StandardScalerParams extends Params with HasInputCol with HasOutputCol {

  /**
   * Centers the data with mean before scaling.
   * It will build a dense output, so this does not work on sparse input
   * and will raise an exception.
   * Default: false
   * @group param
   */
  val withMean: BooleanParam = new BooleanParam(this, "withMean", "Center data with mean")

  /**
   * Scales the data to unit standard deviation.
   * Default: true
   * @group param
   */
  val withStd: BooleanParam = new BooleanParam(this, "withStd", "Scale to unit standard deviation")
}

/**
 * :: Experimental ::
 * Standardizes features by removing the mean and scaling to unit variance using column summary
 * statistics on the samples in the training set.
 */
@Experimental
class StandardScaler(override val uid: String) extends Estimator[StandardScalerModel]
  with StandardScalerParams {

  def this() = this(Identifiable.randomUID("stdScal"))

  setDefault(withMean -> false, withStd -> true)
>>>>>>> upstream/master

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

<<<<<<< HEAD
  override def fit(dataset: DataFrame, paramMap: ParamMap): StandardScalerModel = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = extractParamMap(paramMap)
    val input = dataset.select(map(inputCol)).map { case Row(v: Vector) => v }
    val scaler = new feature.StandardScaler().fit(input)
    val model = new StandardScalerModel(this, map, scaler)
    Params.inheritValues(map, this, model)
    model
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = extractParamMap(paramMap)
    val inputType = schema(map(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${map(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains(map(outputCol)),
      s"Output column ${map(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField(map(outputCol), new VectorUDT, false)
=======
  /** @group setParam */
  def setWithMean(value: Boolean): this.type = set(withMean, value)

  /** @group setParam */
  def setWithStd(value: Boolean): this.type = set(withStd, value)

  override def fit(dataset: DataFrame): StandardScalerModel = {
    transformSchema(dataset.schema, logging = true)
    val input = dataset.select($(inputCol)).map { case Row(v: Vector) => v }
    val scaler = new feature.StandardScaler(withMean = $(withMean), withStd = $(withStd))
    val scalerModel = scaler.fit(input)
    copyValues(new StandardScalerModel(uid, scalerModel).setParent(this))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
>>>>>>> upstream/master
    StructType(outputFields)
  }
}

/**
<<<<<<< HEAD
 * :: AlphaComponent ::
 * Model fitted by [[StandardScaler]].
 */
@AlphaComponent
class StandardScalerModel private[ml] (
    override val parent: StandardScaler,
    override val fittingParamMap: ParamMap,
=======
 * :: Experimental ::
 * Model fitted by [[StandardScaler]].
 */
@Experimental
class StandardScalerModel private[ml] (
    override val uid: String,
>>>>>>> upstream/master
    scaler: feature.StandardScalerModel)
  extends Model[StandardScalerModel] with StandardScalerParams {

  /** @group setParam */
  def setInputCol(value: String): this.type = set(inputCol, value)

  /** @group setParam */
  def setOutputCol(value: String): this.type = set(outputCol, value)

<<<<<<< HEAD
  override def transform(dataset: DataFrame, paramMap: ParamMap): DataFrame = {
    transformSchema(dataset.schema, paramMap, logging = true)
    val map = extractParamMap(paramMap)
    val scale = udf((v: Vector) => { scaler.transform(v) } : Vector)
    dataset.withColumn(map(outputCol), scale(col(map(inputCol))))
  }

  override def transformSchema(schema: StructType, paramMap: ParamMap): StructType = {
    val map = extractParamMap(paramMap)
    val inputType = schema(map(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${map(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains(map(outputCol)),
      s"Output column ${map(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField(map(outputCol), new VectorUDT, false)
=======
  override def transform(dataset: DataFrame): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val scale = udf { scaler.transform _ }
    dataset.withColumn($(outputCol), scale(col($(inputCol))))
  }

  override def transformSchema(schema: StructType): StructType = {
    val inputType = schema($(inputCol)).dataType
    require(inputType.isInstanceOf[VectorUDT],
      s"Input column ${$(inputCol)} must be a vector column")
    require(!schema.fieldNames.contains($(outputCol)),
      s"Output column ${$(outputCol)} already exists.")
    val outputFields = schema.fields :+ StructField($(outputCol), new VectorUDT, false)
>>>>>>> upstream/master
    StructType(outputFields)
  }
}
