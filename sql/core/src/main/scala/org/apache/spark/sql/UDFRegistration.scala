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

import java.util.{List => JList, Map => JMap}

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.{Accumulator, Logging}
import org.apache.spark.api.python.PythonBroadcast
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.api.java._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.{Expression, ScalaUdf}
import org.apache.spark.sql.execution.PythonUDF
import org.apache.spark.sql.types.DataType


/**
 * Functions for registering user-defined functions. Use [[SQLContext.udf]] to access this.
<<<<<<< HEAD
=======
 *
 * @since 1.3.0
>>>>>>> upstream/master
 */
class UDFRegistration private[sql] (sqlContext: SQLContext) extends Logging {

  private val functionRegistry = sqlContext.functionRegistry

  protected[sql] def registerPython(
      name: String,
      command: Array[Byte],
      envVars: JMap[String, String],
      pythonIncludes: JList[String],
      pythonExec: String,
<<<<<<< HEAD
=======
      pythonVer: String,
>>>>>>> upstream/master
      broadcastVars: JList[Broadcast[PythonBroadcast]],
      accumulator: Accumulator[JList[Array[Byte]]],
      stringDataType: String): Unit = {
    log.debug(
      s"""
        | Registering new PythonUDF:
        | name: $name
        | command: ${command.toSeq}
        | envVars: $envVars
        | pythonIncludes: $pythonIncludes
        | pythonExec: $pythonExec
        | dataType: $stringDataType
      """.stripMargin)


    val dataType = sqlContext.parseDataType(stringDataType)

    def builder(e: Seq[Expression]): PythonUDF =
      PythonUDF(
        name,
        command,
        envVars,
        pythonIncludes,
        pythonExec,
<<<<<<< HEAD
=======
        pythonVer,
>>>>>>> upstream/master
        broadcastVars,
        accumulator,
        dataType,
        e)

    functionRegistry.registerFunction(name, builder)
  }

  // scalastyle:off

  /* register 0-22 were generated by this script

    (0 to 22).map { x =>
      val types = (1 to x).foldRight("RT")((i, s) => {s"A$i, $s"})
      val typeTags = (1 to x).map(i => s"A${i}: TypeTag").foldLeft("RT: TypeTag")(_ + ", " + _)
      println(s"""
        /**
         * Register a Scala closure of ${x} arguments as user-defined function (UDF).
         * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
         * @since 1.3.0
>>>>>>> upstream/master
         */
        def register[$typeTags](name: String, func: Function$x[$types]): UserDefinedFunction = {
          val dataType = ScalaReflection.schemaFor[RT].dataType
          def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
          functionRegistry.registerFunction(name, builder)
          UserDefinedFunction(func, dataType)
        }""")
    }

    (1 to 22).foreach { i =>
      val extTypeArgs = (1 to i).map(_ => "_").mkString(", ")
      val anyTypeArgs = (1 to i).map(_ => "Any").mkString(", ")
      val anyCast = s".asInstanceOf[UDF$i[$anyTypeArgs, Any]]"
      val anyParams = (1 to i).map(_ => "_: Any").mkString(", ")
      println(s"""
         |/**
         | * Register a user-defined function with ${i} arguments.
<<<<<<< HEAD
=======
         | * @since 1.3.0
>>>>>>> upstream/master
         | */
         |def register(name: String, f: UDF$i[$extTypeArgs, _], returnType: DataType) = {
         |  functionRegistry.registerFunction(
         |    name,
         |    (e: Seq[Expression]) => ScalaUdf(f$anyCast.call($anyParams), returnType, e))
         |}""".stripMargin)
    }
    */

  /**
   * Register a Scala closure of 0 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag](name: String, func: Function0[RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 1 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag](name: String, func: Function1[A1, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 2 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag](name: String, func: Function2[A1, A2, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 3 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag](name: String, func: Function3[A1, A2, A3, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 4 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag](name: String, func: Function4[A1, A2, A3, A4, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 5 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag](name: String, func: Function5[A1, A2, A3, A4, A5, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 6 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag](name: String, func: Function6[A1, A2, A3, A4, A5, A6, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 7 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag](name: String, func: Function7[A1, A2, A3, A4, A5, A6, A7, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 8 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag](name: String, func: Function8[A1, A2, A3, A4, A5, A6, A7, A8, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 9 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag](name: String, func: Function9[A1, A2, A3, A4, A5, A6, A7, A8, A9, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 10 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag](name: String, func: Function10[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 11 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag](name: String, func: Function11[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 12 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag](name: String, func: Function12[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 13 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag](name: String, func: Function13[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 14 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag](name: String, func: Function14[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 15 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag](name: String, func: Function15[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 16 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag](name: String, func: Function16[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 17 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag](name: String, func: Function17[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 18 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag](name: String, func: Function18[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 19 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag](name: String, func: Function19[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 20 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag](name: String, func: Function20[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 21 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag, A21: TypeTag](name: String, func: Function21[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  /**
   * Register a Scala closure of 22 arguments as user-defined function (UDF).
   * @tparam RT return type of UDF.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register[RT: TypeTag, A1: TypeTag, A2: TypeTag, A3: TypeTag, A4: TypeTag, A5: TypeTag, A6: TypeTag, A7: TypeTag, A8: TypeTag, A9: TypeTag, A10: TypeTag, A11: TypeTag, A12: TypeTag, A13: TypeTag, A14: TypeTag, A15: TypeTag, A16: TypeTag, A17: TypeTag, A18: TypeTag, A19: TypeTag, A20: TypeTag, A21: TypeTag, A22: TypeTag](name: String, func: Function22[A1, A2, A3, A4, A5, A6, A7, A8, A9, A10, A11, A12, A13, A14, A15, A16, A17, A18, A19, A20, A21, A22, RT]): UserDefinedFunction = {
    val dataType = ScalaReflection.schemaFor[RT].dataType
    def builder(e: Seq[Expression]) = ScalaUdf(func, dataType, e)
    functionRegistry.registerFunction(name, builder)
    UserDefinedFunction(func, dataType)
  }

  //////////////////////////////////////////////////////////////////////////////////////////////
  //////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * Register a user-defined function with 1 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF1[_, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF1[Any, Any]].call(_: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 2 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF2[_, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF2[Any, Any, Any]].call(_: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 3 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF3[_, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF3[Any, Any, Any, Any]].call(_: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 4 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF4[_, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF4[Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 5 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF5[_, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF5[Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 6 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF6[_, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF6[Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 7 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF7[_, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF7[Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 8 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF8[_, _, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF8[Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 9 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF9[_, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF9[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 10 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF10[_, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF10[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 11 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF11[_, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF11[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 12 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF12[_, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF12[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 13 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF13[_, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF13[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 14 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF14[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF14[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 15 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF15[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF15[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 16 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF16[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF16[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 17 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF17[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF17[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 18 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF18[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF18[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 19 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF19[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF19[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 20 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF20[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF20[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 21 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF21[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF21[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  /**
   * Register a user-defined function with 22 arguments.
<<<<<<< HEAD
=======
   * @since 1.3.0
>>>>>>> upstream/master
   */
  def register(name: String, f: UDF22[_, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _, _], returnType: DataType) = {
    functionRegistry.registerFunction(
      name,
      (e: Seq[Expression]) => ScalaUdf(f.asInstanceOf[UDF22[Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any, Any]].call(_: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any, _: Any), returnType, e))
  }

  // scalastyle:on
}
