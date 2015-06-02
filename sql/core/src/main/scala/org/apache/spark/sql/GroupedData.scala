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

package org.apache.spark.sql

import scala.collection.JavaConversions._
import scala.language.implicitConversions

import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.catalyst.analysis.Star
import org.apache.spark.sql.catalyst.expressions._
<<<<<<< HEAD
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.types.NumericType

=======
import org.apache.spark.sql.catalyst.plans.logical.{Rollup, Cube, Aggregate}
import org.apache.spark.sql.types.NumericType

/**
 * Companion object for GroupedData
 */
private[sql] object GroupedData {
  def apply(
      df: DataFrame,
      groupingExprs: Seq[Expression],
      groupType: GroupType): GroupedData = {
    new GroupedData(df, groupingExprs, groupType: GroupType)
  }

  /**
   * The Grouping Type
   */
  private[sql] trait GroupType

  /**
   * To indicate it's the GroupBy
   */
  private[sql] object GroupByType extends GroupType

  /**
   * To indicate it's the CUBE
   */
  private[sql] object CubeType extends GroupType

  /**
   * To indicate it's the ROLLUP
   */
  private[sql] object RollupType extends GroupType
}
>>>>>>> upstream/master

/**
 * :: Experimental ::
 * A set of methods for aggregations on a [[DataFrame]], created by [[DataFrame.groupBy]].
<<<<<<< HEAD
 */
@Experimental
class GroupedData protected[sql](df: DataFrame, groupingExprs: Seq[Expression]) {

  private[sql] implicit def toDF(aggExprs: Seq[NamedExpression]): DataFrame = {
    val namedGroupingExprs = groupingExprs.map {
      case expr: NamedExpression => expr
      case expr: Expression => Alias(expr, expr.prettyString)()
    }
    DataFrame(
      df.sqlContext, Aggregate(groupingExprs, namedGroupingExprs ++ aggExprs, df.logicalPlan))
  }

  private[this] def aggregateNumericColumns(colNames: String*)(f: Expression => Expression)
    : Seq[NamedExpression] = {
=======
 *
 * @since 1.3.0
 */
@Experimental
class GroupedData protected[sql](
    df: DataFrame,
    groupingExprs: Seq[Expression],
    private val groupType: GroupedData.GroupType) {

  private[this] def toDF(aggExprs: Seq[NamedExpression]): DataFrame = {
    val aggregates = if (df.sqlContext.conf.dataFrameRetainGroupColumns) {
        val retainedExprs = groupingExprs.map {
          case expr: NamedExpression => expr
          case expr: Expression => Alias(expr, expr.prettyString)()
        }
        retainedExprs ++ aggExprs
      } else {
        aggExprs
      }

    groupType match {
      case GroupedData.GroupByType =>
        DataFrame(
          df.sqlContext, Aggregate(groupingExprs, aggregates, df.logicalPlan))
      case GroupedData.RollupType =>
        DataFrame(
          df.sqlContext, Rollup(groupingExprs, df.logicalPlan, aggregates))
      case GroupedData.CubeType =>
        DataFrame(
          df.sqlContext, Cube(groupingExprs, df.logicalPlan, aggregates))
    }
  }

  private[this] def aggregateNumericColumns(colNames: String*)(f: Expression => Expression)
    : DataFrame = {
>>>>>>> upstream/master

    val columnExprs = if (colNames.isEmpty) {
      // No columns specified. Use all numeric columns.
      df.numericColumns
    } else {
      // Make sure all specified columns are numeric.
      colNames.map { colName =>
        val namedExpr = df.resolve(colName)
        if (!namedExpr.dataType.isInstanceOf[NumericType]) {
          throw new AnalysisException(
            s""""$colName" is not a numeric column. """ +
            "Aggregation function can only be applied on a numeric column.")
        }
        namedExpr
      }
    }
<<<<<<< HEAD
    columnExprs.map { c =>
      val a = f(c)
      Alias(a, a.prettyString)()
    }
=======
    toDF(columnExprs.map { c =>
      val a = f(c)
      Alias(a, a.prettyString)()
    })
>>>>>>> upstream/master
  }

  private[this] def strToExpr(expr: String): (Expression => Expression) = {
    expr.toLowerCase match {
      case "avg" | "average" | "mean" => Average
      case "max" => Max
      case "min" => Min
      case "sum" => Sum
      case "count" | "size" =>
        // Turn count(*) into count(1)
        (inputExpr: Expression) => inputExpr match {
          case s: Star => Count(Literal(1))
          case _ => Count(inputExpr)
        }
    }
  }

  /**
   * (Scala-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting [[DataFrame]] will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(
   *     "age" -> "max",
   *     "expense" -> "sum"
   *   )
   * }}}
<<<<<<< HEAD
=======
   *
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def agg(aggExpr: (String, String), aggExprs: (String, String)*): DataFrame = {
    agg((aggExpr +: aggExprs).toMap)
  }

  /**
   * (Scala-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting [[DataFrame]] will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   df.groupBy("department").agg(Map(
   *     "age" -> "max",
   *     "expense" -> "sum"
   *   ))
   * }}}
<<<<<<< HEAD
   */
  def agg(exprs: Map[String, String]): DataFrame = {
    exprs.map { case (colName, expr) =>
      val a = strToExpr(expr)(df(colName).expr)
      Alias(a, a.prettyString)()
    }.toSeq
=======
   *
   * @since 1.3.0
   */
  def agg(exprs: Map[String, String]): DataFrame = {
    toDF(exprs.map { case (colName, expr) =>
      val a = strToExpr(expr)(df(colName).expr)
      Alias(a, a.prettyString)()
    }.toSeq)
>>>>>>> upstream/master
  }

  /**
   * (Java-specific) Compute aggregates by specifying a map from column name to
   * aggregate methods. The resulting [[DataFrame]] will also contain the grouping columns.
   *
   * The available aggregate methods are `avg`, `max`, `min`, `sum`, `count`.
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *   import com.google.common.collect.ImmutableMap;
   *   df.groupBy("department").agg(ImmutableMap.of("age", "max", "expense", "sum"));
   * }}}
<<<<<<< HEAD
=======
   *
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def agg(exprs: java.util.Map[String, String]): DataFrame = {
    agg(exprs.toMap)
  }

  /**
<<<<<<< HEAD
   * Compute aggregates by specifying a series of aggregate columns. Unlike other methods in this
   * class, the resulting [[DataFrame]] won't automatically include the grouping columns.
=======
   * Compute aggregates by specifying a series of aggregate columns. Note that this function by
   * default retains the grouping columns in its output. To not retain grouping columns, set
   * `spark.sql.retainGroupColumns` to false.
>>>>>>> upstream/master
   *
   * The available aggregate methods are defined in [[org.apache.spark.sql.functions]].
   *
   * {{{
   *   // Selects the age of the oldest employee and the aggregate expense for each department
   *
   *   // Scala:
   *   import org.apache.spark.sql.functions._
<<<<<<< HEAD
   *   df.groupBy("department").agg($"department", max($"age"), sum($"expense"))
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.groupBy("department").agg(col("department"), max(col("age")), sum(col("expense")));
   * }}}
   */
  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame = {
    val aggExprs = (expr +: exprs).map(_.expr).map {
      case expr: NamedExpression => expr
      case expr: Expression => Alias(expr, expr.prettyString)()
    }
    DataFrame(df.sqlContext, Aggregate(groupingExprs, aggExprs, df.logicalPlan))
=======
   *   df.groupBy("department").agg(max("age"), sum("expense"))
   *
   *   // Java:
   *   import static org.apache.spark.sql.functions.*;
   *   df.groupBy("department").agg(max("age"), sum("expense"));
   * }}}
   *
   * Note that before Spark 1.4, the default behavior is to NOT retain grouping columns. To change
   * to that behavior, set config variable `spark.sql.retainGroupColumns` to `false`.
   * {{{
   *   // Scala, 1.3.x:
   *   df.groupBy("department").agg($"department", max("age"), sum("expense"))
   *
   *   // Java, 1.3.x:
   *   df.groupBy("department").agg(col("department"), max("age"), sum("expense"));
   * }}}
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def agg(expr: Column, exprs: Column*): DataFrame = {
    toDF((expr +: exprs).map(_.expr).map {
      case expr: NamedExpression => expr
      case expr: Expression => Alias(expr, expr.prettyString)()
    })
>>>>>>> upstream/master
  }

  /**
   * Count the number of rows for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
<<<<<<< HEAD
   */
  def count(): DataFrame = Seq(Alias(Count(Literal(1)), "count")())
=======
   *
   * @since 1.3.0
   */
  def count(): DataFrame = toDF(Seq(Alias(Count(Literal(1)), "count")()))
>>>>>>> upstream/master

  /**
   * Compute the average value for each numeric columns for each group. This is an alias for `avg`.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the average values for them.
<<<<<<< HEAD
   */
  @scala.annotation.varargs
  def mean(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames:_*)(Average)
  }
 
=======
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def mean(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames : _*)(Average)
  }

>>>>>>> upstream/master
  /**
   * Compute the max value for each numeric columns for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the max values for them.
<<<<<<< HEAD
   */
  @scala.annotation.varargs
  def max(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames:_*)(Max)
=======
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def max(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames : _*)(Max)
>>>>>>> upstream/master
  }

  /**
   * Compute the mean value for each numeric columns for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the mean values for them.
<<<<<<< HEAD
   */
  @scala.annotation.varargs
  def avg(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames:_*)(Average)
=======
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def avg(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames : _*)(Average)
>>>>>>> upstream/master
  }

  /**
   * Compute the min value for each numeric column for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the min values for them.
<<<<<<< HEAD
   */
  @scala.annotation.varargs
  def min(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames:_*)(Min)
=======
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def min(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames : _*)(Min)
>>>>>>> upstream/master
  }

  /**
   * Compute the sum for each numeric columns for each group.
   * The resulting [[DataFrame]] will also contain the grouping columns.
   * When specified columns are given, only compute the sum for them.
<<<<<<< HEAD
   */
  @scala.annotation.varargs
  def sum(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames:_*)(Sum)
  }    
=======
   *
   * @since 1.3.0
   */
  @scala.annotation.varargs
  def sum(colNames: String*): DataFrame = {
    aggregateNumericColumns(colNames : _*)(Sum)
  }
>>>>>>> upstream/master
}
