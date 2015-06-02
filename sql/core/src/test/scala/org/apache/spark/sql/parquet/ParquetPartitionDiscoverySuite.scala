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
package org.apache.spark.sql.parquet

<<<<<<< HEAD
=======
import java.io.File
import java.math.BigInteger
import java.sql.{Timestamp, Date}

>>>>>>> upstream/master
import scala.collection.mutable.ArrayBuffer

import org.apache.hadoop.fs.Path

import org.apache.spark.sql.catalyst.expressions.Literal
<<<<<<< HEAD
import org.apache.spark.sql.parquet.ParquetRelation2._
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{QueryTest, Row, SQLContext}
=======
import org.apache.spark.sql.sources.PartitioningUtils._
import org.apache.spark.sql.sources.{LogicalRelation, Partition, PartitionSpec}
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Column, QueryTest, Row, SQLContext}
>>>>>>> upstream/master

// The data where the partitioning key exists only in the directory structure.
case class ParquetData(intField: Int, stringField: String)

// The data that also includes the partitioning key
case class ParquetDataWithKey(intField: Int, pi: Int, stringField: String, ps: String)

class ParquetPartitionDiscoverySuite extends QueryTest with ParquetTest {
  override val sqlContext: SQLContext = TestSQLContext

  import sqlContext._
  import sqlContext.implicits._

<<<<<<< HEAD
  val defaultPartitionName = "__NULL__"
=======
  val defaultPartitionName = "__HIVE_DEFAULT_PARTITION__"
>>>>>>> upstream/master

  test("column type inference") {
    def check(raw: String, literal: Literal): Unit = {
      assert(inferPartitionColumnValue(raw, defaultPartitionName) === literal)
    }

    check("10", Literal.create(10, IntegerType))
    check("1000000000000000", Literal.create(1000000000000000L, LongType))
    check("1.5", Literal.create(1.5, FloatType))
    check("hello", Literal.create("hello", StringType))
    check(defaultPartitionName, Literal.create(null, NullType))
  }

  test("parse partition") {
<<<<<<< HEAD
    def check(path: String, expected: PartitionValues): Unit = {
=======
    def check(path: String, expected: Option[PartitionValues]): Unit = {
>>>>>>> upstream/master
      assert(expected === parsePartition(new Path(path), defaultPartitionName))
    }

    def checkThrows[T <: Throwable: Manifest](path: String, expected: String): Unit = {
      val message = intercept[T] {
<<<<<<< HEAD
        parsePartition(new Path(path), defaultPartitionName)
=======
        parsePartition(new Path(path), defaultPartitionName).get
>>>>>>> upstream/master
      }.getMessage

      assert(message.contains(expected))
    }

<<<<<<< HEAD
    check(
      "file:///",
      PartitionValues(
        ArrayBuffer.empty[String],
        ArrayBuffer.empty[Literal]))

    check(
      "file://path/a=10",
      PartitionValues(
        ArrayBuffer("a"),
        ArrayBuffer(Literal.create(10, IntegerType))))

    check(
      "file://path/a=10/b=hello/c=1.5",
=======
    check("file://path/a=10", Some {
      PartitionValues(
        ArrayBuffer("a"),
        ArrayBuffer(Literal.create(10, IntegerType)))
    })

    check("file://path/a=10/b=hello/c=1.5", Some {
>>>>>>> upstream/master
      PartitionValues(
        ArrayBuffer("a", "b", "c"),
        ArrayBuffer(
          Literal.create(10, IntegerType),
          Literal.create("hello", StringType),
<<<<<<< HEAD
          Literal.create(1.5, FloatType))))

    check(
      "file://path/a=10/b_hello/c=1.5",
      PartitionValues(
        ArrayBuffer("c"),
        ArrayBuffer(Literal.create(1.5, FloatType))))
=======
          Literal.create(1.5, FloatType)))
    })

    check("file://path/a=10/b_hello/c=1.5", Some {
      PartitionValues(
        ArrayBuffer("c"),
        ArrayBuffer(Literal.create(1.5, FloatType)))
    })

    check("file:///", None)
    check("file:///path/_temporary", None)
    check("file:///path/_temporary/c=1.5", None)
    check("file:///path/_temporary/path", None)
    check("file://path/a=10/_temporary/c=1.5", None)
    check("file://path/a=10/c=1.5/_temporary", None)
>>>>>>> upstream/master

    checkThrows[AssertionError]("file://path/=10", "Empty partition column name")
    checkThrows[AssertionError]("file://path/a=", "Empty partition column value")
  }

  test("parse partitions") {
    def check(paths: Seq[String], spec: PartitionSpec): Unit = {
      assert(parsePartitions(paths.map(new Path(_)), defaultPartitionName) === spec)
    }

    check(Seq(
      "hdfs://host:9000/path/a=10/b=hello"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType))),
        Seq(Partition(Row(10, "hello"), "hdfs://host:9000/path/a=10/b=hello"))))

    check(Seq(
      "hdfs://host:9000/path/a=10/b=20",
      "hdfs://host:9000/path/a=10.5/b=hello"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", FloatType),
          StructField("b", StringType))),
        Seq(
          Partition(Row(10, "20"), "hdfs://host:9000/path/a=10/b=20"),
          Partition(Row(10.5, "hello"), "hdfs://host:9000/path/a=10.5/b=hello"))))

    check(Seq(
<<<<<<< HEAD
=======
      "hdfs://host:9000/path/_temporary",
      "hdfs://host:9000/path/a=10/b=20",
      "hdfs://host:9000/path/a=10.5/b=hello",
      "hdfs://host:9000/path/a=10.5/_temporary",
      "hdfs://host:9000/path/a=10.5/_TeMpOrArY",
      "hdfs://host:9000/path/a=10.5/b=hello/_temporary",
      "hdfs://host:9000/path/a=10.5/b=hello/_TEMPORARY",
      "hdfs://host:9000/path/_temporary/path",
      "hdfs://host:9000/path/a=11/_temporary/path",
      "hdfs://host:9000/path/a=10.5/b=world/_temporary/path"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", FloatType),
          StructField("b", StringType))),
        Seq(
          Partition(Row(10, "20"), "hdfs://host:9000/path/a=10/b=20"),
          Partition(Row(10.5, "hello"), "hdfs://host:9000/path/a=10.5/b=hello"))))

    check(Seq(
>>>>>>> upstream/master
      s"hdfs://host:9000/path/a=10/b=20",
      s"hdfs://host:9000/path/a=$defaultPartitionName/b=hello"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", IntegerType),
          StructField("b", StringType))),
        Seq(
          Partition(Row(10, "20"), s"hdfs://host:9000/path/a=10/b=20"),
          Partition(Row(null, "hello"), s"hdfs://host:9000/path/a=$defaultPartitionName/b=hello"))))

    check(Seq(
      s"hdfs://host:9000/path/a=10/b=$defaultPartitionName",
      s"hdfs://host:9000/path/a=10.5/b=$defaultPartitionName"),
      PartitionSpec(
        StructType(Seq(
          StructField("a", FloatType),
          StructField("b", StringType))),
        Seq(
          Partition(Row(10, null), s"hdfs://host:9000/path/a=10/b=$defaultPartitionName"),
          Partition(Row(10.5, null), s"hdfs://host:9000/path/a=10.5/b=$defaultPartitionName"))))
<<<<<<< HEAD
=======

    check(Seq(
      s"hdfs://host:9000/path1",
      s"hdfs://host:9000/path2"),
      PartitionSpec.emptySpec)
>>>>>>> upstream/master
  }

  test("read partitioned table - normal case") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", "bar")
      } {
<<<<<<< HEAD
        makeParquetFile(
          (1 to 10).map(i => ParquetData(i, i.toString)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

      parquetFile(base.getCanonicalPath).registerTempTable("t")
=======
        val dir = makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps)
        makeParquetFile(
          (1 to 10).map(i => ParquetData(i, i.toString)),
          dir)
        // Introduce _temporary dir to test the robustness of the schema discovery process.
        new File(dir.toString, "_temporary").mkdir()
      }
      // Introduce _temporary dir to the base dir the robustness of the schema discovery process.
      new File(base.getCanonicalPath, "_temporary").mkdir()

      println("load the partitioned table")
      read.parquet(base.getCanonicalPath).registerTempTable("t")
>>>>>>> upstream/master

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", "bar")
          } yield Row(i, i.toString, pi, ps))

        checkAnswer(
          sql("SELECT intField, pi FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            _ <- Seq("foo", "bar")
          } yield Row(i, pi))

        checkAnswer(
          sql("SELECT * FROM t WHERE pi = 1"),
          for {
            i <- 1 to 10
            ps <- Seq("foo", "bar")
          } yield Row(i, i.toString, 1, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps = 'foo'"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, i.toString, pi, "foo"))
      }
    }
  }

  test("read partitioned table - partition key included in Parquet file") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", "bar")
      } {
        makeParquetFile(
          (1 to 10).map(i => ParquetDataWithKey(i, pi, i.toString, ps)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

<<<<<<< HEAD
      parquetFile(base.getCanonicalPath).registerTempTable("t")
=======
      read.parquet(base.getCanonicalPath).registerTempTable("t")
>>>>>>> upstream/master

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", "bar")
          } yield Row(i, pi, i.toString, ps))

        checkAnswer(
          sql("SELECT intField, pi FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            _ <- Seq("foo", "bar")
          } yield Row(i, pi))

        checkAnswer(
          sql("SELECT * FROM t WHERE pi = 1"),
          for {
            i <- 1 to 10
            ps <- Seq("foo", "bar")
          } yield Row(i, 1, i.toString, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps = 'foo'"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, pi, i.toString, "foo"))
      }
    }
  }

  test("read partitioned table - with nulls") {
    withTempDir { base =>
      for {
        // Must be `Integer` rather than `Int` here. `null.asInstanceOf[Int]` results in a zero...
        pi <- Seq(1, null.asInstanceOf[Integer])
        ps <- Seq("foo", null.asInstanceOf[String])
      } {
        makeParquetFile(
          (1 to 10).map(i => ParquetData(i, i.toString)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

<<<<<<< HEAD
      val parquetRelation = load(
        "org.apache.spark.sql.parquet",
        Map(
          "path" -> base.getCanonicalPath,
          ParquetRelation2.DEFAULT_PARTITION_NAME -> defaultPartitionName))

=======
      val parquetRelation = read.format("org.apache.spark.sql.parquet").load(base.getCanonicalPath)
>>>>>>> upstream/master
      parquetRelation.registerTempTable("t")

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, null.asInstanceOf[Integer])
            ps <- Seq("foo", null.asInstanceOf[String])
          } yield Row(i, i.toString, pi, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE pi IS NULL"),
          for {
            i <- 1 to 10
            ps <- Seq("foo", null.asInstanceOf[String])
          } yield Row(i, i.toString, null, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps IS NULL"),
          for {
            i <- 1 to 10
            pi <- Seq(1, null.asInstanceOf[Integer])
          } yield Row(i, i.toString, pi, null))
      }
    }
  }

  test("read partitioned table - with nulls and partition keys are included in Parquet file") {
    withTempDir { base =>
      for {
        pi <- Seq(1, 2)
        ps <- Seq("foo", null.asInstanceOf[String])
      } {
        makeParquetFile(
          (1 to 10).map(i => ParquetDataWithKey(i, pi, i.toString, ps)),
          makePartitionDir(base, defaultPartitionName, "pi" -> pi, "ps" -> ps))
      }

<<<<<<< HEAD
      val parquetRelation = load(
        "org.apache.spark.sql.parquet",
        Map(
          "path" -> base.getCanonicalPath,
          ParquetRelation2.DEFAULT_PARTITION_NAME -> defaultPartitionName))

=======
      val parquetRelation = read.format("org.apache.spark.sql.parquet").load(base.getCanonicalPath)
>>>>>>> upstream/master
      parquetRelation.registerTempTable("t")

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
            ps <- Seq("foo", null.asInstanceOf[String])
          } yield Row(i, pi, i.toString, ps))

        checkAnswer(
          sql("SELECT * FROM t WHERE ps IS NULL"),
          for {
            i <- 1 to 10
            pi <- Seq(1, 2)
          } yield Row(i, pi, i.toString, null))
      }
    }
  }

  test("read partitioned table - merging compatible schemas") {
    withTempDir { base =>
      makeParquetFile(
        (1 to 10).map(i => Tuple1(i)).toDF("intField"),
        makePartitionDir(base, defaultPartitionName, "pi" -> 1))

      makeParquetFile(
        (1 to 10).map(i => (i, i.toString)).toDF("intField", "stringField"),
        makePartitionDir(base, defaultPartitionName, "pi" -> 2))

<<<<<<< HEAD
      load(base.getCanonicalPath, "org.apache.spark.sql.parquet").registerTempTable("t")
=======
      read.format("org.apache.spark.sql.parquet").load(base.getCanonicalPath).registerTempTable("t")
>>>>>>> upstream/master

      withTempTable("t") {
        checkAnswer(
          sql("SELECT * FROM t"),
          (1 to 10).map(i => Row(i, null, 1)) ++ (1 to 10).map(i => Row(i, i.toString, 2)))
      }
    }
  }
<<<<<<< HEAD
=======

  test("SPARK-7749 Non-partitioned table should have empty partition spec") {
    withTempPath { dir =>
      (1 to 10).map(i => (i, i.toString)).toDF("a", "b").write.parquet(dir.getCanonicalPath)
      val queryExecution = read.parquet(dir.getCanonicalPath).queryExecution
      queryExecution.analyzed.collectFirst {
        case LogicalRelation(relation: ParquetRelation2) =>
          assert(relation.partitionSpec === PartitionSpec.emptySpec)
      }.getOrElse {
        fail(s"Expecting a ParquetRelation2, but got:\n$queryExecution")
      }
    }
  }

  test("SPARK-7847: Dynamic partition directory path escaping and unescaping") {
    withTempPath { dir =>
      val df = Seq("/", "[]", "?").zipWithIndex.map(_.swap).toDF("i", "s")
      df.write.format("parquet").partitionBy("s").save(dir.getCanonicalPath)
      checkAnswer(read.parquet(dir.getCanonicalPath), df.collect())
    }
  }

  test("Various partition value types") {
    val row =
      Row(
        100.toByte,
        40000.toShort,
        Int.MaxValue,
        Long.MaxValue,
        1.5.toFloat,
        4.5,
        new java.math.BigDecimal(new BigInteger("212500"), 5),
        new java.math.BigDecimal(2.125),
        java.sql.Date.valueOf("2015-05-23"),
        new Timestamp(0),
        "This is a string, /[]?=:",
        "This is not a partition column")

    // BooleanType is not supported yet
    val partitionColumnTypes =
      Seq(
        ByteType,
        ShortType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        DecimalType(10, 5),
        DecimalType.Unlimited,
        DateType,
        TimestampType,
        StringType)

    val partitionColumns = partitionColumnTypes.zipWithIndex.map {
      case (t, index) => StructField(s"p_$index", t)
    }

    val schema = StructType(partitionColumns :+ StructField(s"i", StringType))
    val df = createDataFrame(sparkContext.parallelize(row :: Nil), schema)

    withTempPath { dir =>
      df.write.format("parquet").partitionBy(partitionColumns.map(_.name): _*).save(dir.toString)
      val fields = schema.map(f => Column(f.name).cast(f.dataType))
      checkAnswer(read.load(dir.toString).select(fields: _*), row)
    }
  }
>>>>>>> upstream/master
}
