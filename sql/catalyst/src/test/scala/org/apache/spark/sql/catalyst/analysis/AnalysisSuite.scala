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

package org.apache.spark.sql.catalyst.analysis

import org.scalatest.BeforeAndAfter

<<<<<<< HEAD
=======
import org.apache.spark.SparkFunSuite
>>>>>>> upstream/master
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.types._
<<<<<<< HEAD

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._

class AnalysisSuite extends FunSuite with BeforeAndAfter {
  val caseSensitiveCatalog = new SimpleCatalog(true)
  val caseInsensitiveCatalog = new SimpleCatalog(false)

  val caseSensitiveAnalyzer =
    new Analyzer(caseSensitiveCatalog, EmptyFunctionRegistry, caseSensitive = true) {
      override val extendedResolutionRules = EliminateSubQueries :: Nil
    }
  val caseInsensitiveAnalyzer =
    new Analyzer(caseInsensitiveCatalog, EmptyFunctionRegistry, caseSensitive = false) {
      override val extendedResolutionRules = EliminateSubQueries :: Nil
    }


  def caseSensitiveAnalyze(plan: LogicalPlan): Unit =
    caseSensitiveAnalyzer.checkAnalysis(caseSensitiveAnalyzer(plan))

  def caseInsensitiveAnalyze(plan: LogicalPlan): Unit =
    caseInsensitiveAnalyzer.checkAnalysis(caseInsensitiveAnalyzer(plan))
=======
import org.apache.spark.sql.catalyst.SimpleCatalystConf
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._

class AnalysisSuite extends SparkFunSuite with BeforeAndAfter {
  val caseSensitiveConf = new SimpleCatalystConf(true)
  val caseInsensitiveConf = new SimpleCatalystConf(false)

  val caseSensitiveCatalog = new SimpleCatalog(caseSensitiveConf)
  val caseInsensitiveCatalog = new SimpleCatalog(caseInsensitiveConf)

  val caseSensitiveAnalyzer =
    new Analyzer(caseSensitiveCatalog, EmptyFunctionRegistry, caseSensitiveConf) {
      override val extendedResolutionRules = EliminateSubQueries :: Nil
    }
  val caseInsensitiveAnalyzer =
    new Analyzer(caseInsensitiveCatalog, EmptyFunctionRegistry, caseInsensitiveConf) {
      override val extendedResolutionRules = EliminateSubQueries :: Nil
    }

  def caseSensitiveAnalyze(plan: LogicalPlan): Unit =
    caseSensitiveAnalyzer.checkAnalysis(caseSensitiveAnalyzer.execute(plan))

  def caseInsensitiveAnalyze(plan: LogicalPlan): Unit =
    caseInsensitiveAnalyzer.checkAnalysis(caseInsensitiveAnalyzer.execute(plan))
>>>>>>> upstream/master

  val testRelation = LocalRelation(AttributeReference("a", IntegerType, nullable = true)())
  val testRelation2 = LocalRelation(
    AttributeReference("a", StringType)(),
    AttributeReference("b", StringType)(),
    AttributeReference("c", DoubleType)(),
    AttributeReference("d", DecimalType.Unlimited)(),
    AttributeReference("e", ShortType)())

  val nestedRelation = LocalRelation(
    AttributeReference("top", StructType(
      StructField("duplicateField", StringType) ::
      StructField("duplicateField", StringType) ::
      StructField("differentCase", StringType) ::
      StructField("differentcase", StringType) :: Nil
    ))())

  val nestedRelation2 = LocalRelation(
    AttributeReference("top", StructType(
      StructField("aField", StringType) ::
      StructField("bField", StringType) ::
      StructField("cField", StringType) :: Nil
    ))())

<<<<<<< HEAD
=======
  val listRelation = LocalRelation(
    AttributeReference("list", ArrayType(IntegerType))())

>>>>>>> upstream/master
  before {
    caseSensitiveCatalog.registerTable(Seq("TaBlE"), testRelation)
    caseInsensitiveCatalog.registerTable(Seq("TaBlE"), testRelation)
  }

  test("union project *") {
    val plan = (1 to 100)
      .map(_ => testRelation)
      .fold[LogicalPlan](testRelation) { (a, b) =>
        a.select(UnresolvedStar(None)).select('a).unionAll(b.select(UnresolvedStar(None)))
      }

<<<<<<< HEAD
    assert(caseInsensitiveAnalyzer(plan).resolved)
=======
    assert(caseInsensitiveAnalyzer.execute(plan).resolved)
>>>>>>> upstream/master
  }

  test("check project's resolved") {
    assert(Project(testRelation.output, testRelation).resolved)

    assert(!Project(Seq(UnresolvedAttribute("a")), testRelation).resolved)

<<<<<<< HEAD
    val explode = Explode(Nil, AttributeReference("a", IntegerType, nullable = true)())
=======
    val explode = Explode(AttributeReference("a", IntegerType, nullable = true)())
>>>>>>> upstream/master
    assert(!Project(Seq(Alias(explode, "explode")()), testRelation).resolved)

    assert(!Project(Seq(Alias(Count(Literal(1)), "count")()), testRelation).resolved)
  }

  test("analyze project") {
    assert(
<<<<<<< HEAD
      caseSensitiveAnalyzer(Project(Seq(UnresolvedAttribute("a")), testRelation)) ===
        Project(testRelation.output, testRelation))

    assert(
      caseSensitiveAnalyzer(
=======
      caseSensitiveAnalyzer.execute(Project(Seq(UnresolvedAttribute("a")), testRelation)) ===
        Project(testRelation.output, testRelation))

    assert(
      caseSensitiveAnalyzer.execute(
>>>>>>> upstream/master
        Project(Seq(UnresolvedAttribute("TbL.a")),
          UnresolvedRelation(Seq("TaBlE"), Some("TbL")))) ===
        Project(testRelation.output, testRelation))

    val e = intercept[AnalysisException] {
      caseSensitiveAnalyze(
        Project(Seq(UnresolvedAttribute("tBl.a")),
          UnresolvedRelation(Seq("TaBlE"), Some("TbL"))))
    }
    assert(e.getMessage().toLowerCase.contains("cannot resolve"))

    assert(
<<<<<<< HEAD
      caseInsensitiveAnalyzer(
=======
      caseInsensitiveAnalyzer.execute(
>>>>>>> upstream/master
        Project(Seq(UnresolvedAttribute("TbL.a")),
          UnresolvedRelation(Seq("TaBlE"), Some("TbL")))) ===
        Project(testRelation.output, testRelation))

    assert(
<<<<<<< HEAD
      caseInsensitiveAnalyzer(
=======
      caseInsensitiveAnalyzer.execute(
>>>>>>> upstream/master
        Project(Seq(UnresolvedAttribute("tBl.a")),
          UnresolvedRelation(Seq("TaBlE"), Some("TbL")))) ===
        Project(testRelation.output, testRelation))
  }

  test("resolve relations") {
    val e = intercept[RuntimeException] {
      caseSensitiveAnalyze(UnresolvedRelation(Seq("tAbLe"), None))
    }
    assert(e.getMessage == "Table Not Found: tAbLe")

    assert(
<<<<<<< HEAD
      caseSensitiveAnalyzer(UnresolvedRelation(Seq("TaBlE"), None)) === testRelation)

    assert(
      caseInsensitiveAnalyzer(UnresolvedRelation(Seq("tAbLe"), None)) === testRelation)

    assert(
      caseInsensitiveAnalyzer(UnresolvedRelation(Seq("TaBlE"), None)) === testRelation)
=======
      caseSensitiveAnalyzer.execute(UnresolvedRelation(Seq("TaBlE"), None)) === testRelation)

    assert(
      caseInsensitiveAnalyzer.execute(UnresolvedRelation(Seq("tAbLe"), None)) === testRelation)

    assert(
      caseInsensitiveAnalyzer.execute(UnresolvedRelation(Seq("TaBlE"), None)) === testRelation)
>>>>>>> upstream/master
  }

  def errorTest(
      name: String,
      plan: LogicalPlan,
      errorMessages: Seq[String],
      caseSensitive: Boolean = true): Unit = {
    test(name) {
      val error = intercept[AnalysisException] {
<<<<<<< HEAD
        if(caseSensitive) {
=======
        if (caseSensitive) {
>>>>>>> upstream/master
          caseSensitiveAnalyze(plan)
        } else {
          caseInsensitiveAnalyze(plan)
        }
      }

<<<<<<< HEAD
      errorMessages.foreach(m => assert(error.getMessage contains m))
=======
      errorMessages.foreach(m => assert(error.getMessage.toLowerCase contains m.toLowerCase))
>>>>>>> upstream/master
    }
  }

  errorTest(
<<<<<<< HEAD
=======
    "unresolved window function",
    testRelation2.select(
      WindowExpression(
        UnresolvedWindowFunction(
          "lead",
          UnresolvedAttribute("c") :: Nil),
        WindowSpecDefinition(
          UnresolvedAttribute("a") :: Nil,
          SortOrder(UnresolvedAttribute("b"), Ascending) :: Nil,
          UnspecifiedFrame)).as('window)),
      "lead" :: "window functions currently requires a HiveContext" :: Nil)

  errorTest(
    "too many generators",
    listRelation.select(Explode('list).as('a), Explode('list).as('b)),
    "only one generator" :: "explode" :: Nil)

  errorTest(
>>>>>>> upstream/master
    "unresolved attributes",
    testRelation.select('abcd),
    "cannot resolve" :: "abcd" :: Nil)

  errorTest(
    "bad casts",
    testRelation.select(Literal(1).cast(BinaryType).as('badCast)),
    "invalid cast" :: Literal(1).dataType.simpleString :: BinaryType.simpleString :: Nil)

  errorTest(
    "non-boolean filters",
    testRelation.where(Literal(1)),
    "filter" :: "'1'" :: "not a boolean" :: Literal(1).dataType.simpleString :: Nil)

  errorTest(
    "missing group by",
    testRelation2.groupBy('a)('b),
    "'b'" :: "group by" :: Nil
  )

  errorTest(
    "ambiguous field",
    nestedRelation.select($"top.duplicateField"),
    "Ambiguous reference to fields" :: "duplicateField" :: Nil,
    caseSensitive = false)

  errorTest(
    "ambiguous field due to case insensitivity",
    nestedRelation.select($"top.differentCase"),
    "Ambiguous reference to fields" :: "differentCase" :: "differentcase" :: Nil,
    caseSensitive = false)

  errorTest(
    "missing field",
    nestedRelation2.select($"top.c"),
    "No such struct field" :: "aField" :: "bField" :: "cField" :: Nil,
    caseSensitive = false)

  case class UnresolvedTestPlan() extends LeafNode {
    override lazy val resolved = false
    override def output: Seq[Attribute] = Nil
  }

  errorTest(
    "catch all unresolved plan",
    UnresolvedTestPlan(),
    "unresolved" :: Nil)


  test("divide should be casted into fractional types") {
    val testRelation2 = LocalRelation(
      AttributeReference("a", StringType)(),
      AttributeReference("b", StringType)(),
      AttributeReference("c", DoubleType)(),
      AttributeReference("d", DecimalType.Unlimited)(),
      AttributeReference("e", ShortType)())

<<<<<<< HEAD
    val plan = caseInsensitiveAnalyzer(
=======
    val plan = caseInsensitiveAnalyzer.execute(
>>>>>>> upstream/master
      testRelation2.select(
        'a / Literal(2) as 'div1,
        'a / 'b as 'div2,
        'a / 'c as 'div3,
        'a / 'd as 'div4,
        'e / 'e as 'div5))
    val pl = plan.asInstanceOf[Project].projectList

    assert(pl(0).dataType == DoubleType)
    assert(pl(1).dataType == DoubleType)
    assert(pl(2).dataType == DoubleType)
    assert(pl(3).dataType == DecimalType.Unlimited)
    assert(pl(4).dataType == DoubleType)
  }

  test("SPARK-6452 regression test") {
    // CheckAnalysis should throw AnalysisException when Aggregate contains missing attribute(s)
    val plan =
      Aggregate(
        Nil,
        Alias(Sum(AttributeReference("a", StringType)(exprId = ExprId(1))), "b")() :: Nil,
        LocalRelation(
          AttributeReference("a", StringType)(exprId = ExprId(2))))

    assert(plan.resolved)

    val message = intercept[AnalysisException] {
      caseSensitiveAnalyze(plan)
    }.getMessage

    assert(message.contains("resolved attribute(s) a#1 missing from a#2"))
  }
}
