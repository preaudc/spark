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

package org.apache.spark.sql.jdbc

import java.sql.{Connection, DriverManager, ResultSet, ResultSetMetaData, SQLException}
import java.util.Properties

<<<<<<< HEAD
import org.apache.commons.lang.StringEscapeUtils.escapeSql
import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Row, SpecificMutableRow}
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._

private[sql] object JDBCRDD extends Logging {
  /**
   * Maps a JDBC type to a Catalyst type.  This function is called only when
   * the DriverQuirks class corresponding to your database driver returns null.
=======
import org.apache.commons.lang3.StringUtils

import org.apache.spark.{Logging, Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.{Row, SpecificMutableRow}
import org.apache.spark.sql.catalyst.util.DateUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.sources._

/**
 * Data corresponding to one partition of a JDBCRDD.
 */
private[sql] case class JDBCPartition(whereClause: String, idx: Int) extends Partition {
  override def index: Int = idx
}


private[sql] object JDBCRDD extends Logging {

  /**
   * Maps a JDBC type to a Catalyst type.  This function is called only when
   * the JdbcDialect class corresponding to your database driver returns null.
>>>>>>> upstream/master
   *
   * @param sqlType - A field of java.sql.Types
   * @return The Catalyst type corresponding to sqlType.
   */
<<<<<<< HEAD
  private def getCatalystType(sqlType: Int): DataType = {
    val answer = sqlType match {
      case java.sql.Types.ARRAY         => null
      case java.sql.Types.BIGINT        => LongType
      case java.sql.Types.BINARY        => BinaryType
      case java.sql.Types.BIT           => BooleanType // Per JDBC; Quirks handles quirky drivers.
=======
  private def getCatalystType(
      sqlType: Int,
      precision: Int,
      scale: Int,
      signed: Boolean): DataType = {
    val answer = sqlType match {
      // scalastyle:off
      case java.sql.Types.ARRAY         => null
      case java.sql.Types.BIGINT        => if (signed) { LongType } else { DecimalType.Unlimited }
      case java.sql.Types.BINARY        => BinaryType
      case java.sql.Types.BIT           => BooleanType // @see JdbcDialect for quirks
>>>>>>> upstream/master
      case java.sql.Types.BLOB          => BinaryType
      case java.sql.Types.BOOLEAN       => BooleanType
      case java.sql.Types.CHAR          => StringType
      case java.sql.Types.CLOB          => StringType
      case java.sql.Types.DATALINK      => null
      case java.sql.Types.DATE          => DateType
<<<<<<< HEAD
=======
      case java.sql.Types.DECIMAL
        if precision != 0 || scale != 0 => DecimalType(precision, scale)
>>>>>>> upstream/master
      case java.sql.Types.DECIMAL       => DecimalType.Unlimited
      case java.sql.Types.DISTINCT      => null
      case java.sql.Types.DOUBLE        => DoubleType
      case java.sql.Types.FLOAT         => FloatType
<<<<<<< HEAD
      case java.sql.Types.INTEGER       => IntegerType
=======
      case java.sql.Types.INTEGER       => if (signed) { IntegerType } else { LongType }
>>>>>>> upstream/master
      case java.sql.Types.JAVA_OBJECT   => null
      case java.sql.Types.LONGNVARCHAR  => StringType
      case java.sql.Types.LONGVARBINARY => BinaryType
      case java.sql.Types.LONGVARCHAR   => StringType
      case java.sql.Types.NCHAR         => StringType
      case java.sql.Types.NCLOB         => StringType
      case java.sql.Types.NULL          => null
<<<<<<< HEAD
      case java.sql.Types.NUMERIC       => DecimalType.Unlimited
=======
      case java.sql.Types.NUMERIC
        if precision != 0 || scale != 0 => DecimalType(precision, scale)
      case java.sql.Types.NUMERIC       => DecimalType.Unlimited
      case java.sql.Types.NVARCHAR      => StringType
>>>>>>> upstream/master
      case java.sql.Types.OTHER         => null
      case java.sql.Types.REAL          => DoubleType
      case java.sql.Types.REF           => StringType
      case java.sql.Types.ROWID         => LongType
      case java.sql.Types.SMALLINT      => IntegerType
      case java.sql.Types.SQLXML        => StringType
      case java.sql.Types.STRUCT        => StringType
      case java.sql.Types.TIME          => TimestampType
      case java.sql.Types.TIMESTAMP     => TimestampType
      case java.sql.Types.TINYINT       => IntegerType
      case java.sql.Types.VARBINARY     => BinaryType
      case java.sql.Types.VARCHAR       => StringType
<<<<<<< HEAD
      case _ => null
=======
      case _                            => null
      // scalastyle:on
>>>>>>> upstream/master
    }

    if (answer == null) throw new SQLException("Unsupported type " + sqlType)
    answer
  }

  /**
   * Takes a (schema, table) specification and returns the table's Catalyst
   * schema.
   *
   * @param url - The JDBC url to fetch information from.
   * @param table - The table name of the desired table.  This may also be a
   *   SQL query wrapped in parentheses.
   *
   * @return A StructType giving the table's Catalyst schema.
   * @throws SQLException if the table specification is garbage.
   * @throws SQLException if the table contains an unsupported type.
   */
  def resolveTable(url: String, table: String, properties: Properties): StructType = {
<<<<<<< HEAD
    val quirks = DriverQuirks.get(url)
=======
    val dialect = JdbcDialects.get(url)
>>>>>>> upstream/master
    val conn: Connection = DriverManager.getConnection(url, properties)
    try {
      val rs = conn.prepareStatement(s"SELECT * FROM $table WHERE 1=0").executeQuery()
      try {
        val rsmd = rs.getMetaData
        val ncols = rsmd.getColumnCount
        val fields = new Array[StructField](ncols)
        var i = 0
        while (i < ncols) {
<<<<<<< HEAD
          val columnName = rsmd.getColumnName(i + 1)
          val dataType = rsmd.getColumnType(i + 1)
          val typeName = rsmd.getColumnTypeName(i + 1)
          val fieldSize = rsmd.getPrecision(i + 1)
          val nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
          val metadata = new MetadataBuilder().putString("name", columnName)
          var columnType = quirks.getCatalystType(dataType, typeName, fieldSize, metadata)
          if (columnType == null) columnType = getCatalystType(dataType)
=======
          val columnName = rsmd.getColumnLabel(i + 1)
          val dataType = rsmd.getColumnType(i + 1)
          val typeName = rsmd.getColumnTypeName(i + 1)
          val fieldSize = rsmd.getPrecision(i + 1)
          val fieldScale = rsmd.getScale(i + 1)
          val isSigned = rsmd.isSigned(i + 1)
          val nullable = rsmd.isNullable(i + 1) != ResultSetMetaData.columnNoNulls
          val metadata = new MetadataBuilder().putString("name", columnName)
          val columnType =
            dialect.getCatalystType(dataType, typeName, fieldSize, metadata).getOrElse(
              getCatalystType(dataType, fieldSize, fieldScale, isSigned))
>>>>>>> upstream/master
          fields(i) = StructField(columnName, columnType, nullable, metadata.build())
          i = i + 1
        }
        return new StructType(fields)
      } finally {
        rs.close()
      }
    } finally {
      conn.close()
    }

    throw new RuntimeException("This line is unreachable.")
  }

  /**
   * Prune all but the specified columns from the specified Catalyst schema.
   *
   * @param schema - The Catalyst schema of the master table
   * @param columns - The list of desired columns
   *
   * @return A Catalyst schema corresponding to columns in the given order.
   */
  private def pruneSchema(schema: StructType, columns: Array[String]): StructType = {
    val fieldMap = Map(schema.fields map { x => x.metadata.getString("name") -> x }: _*)
    new StructType(columns map { name => fieldMap(name) })
  }

  /**
   * Given a driver string and an url, return a function that loads the
   * specified driver string then returns a connection to the JDBC url.
   * getConnector is run on the driver code, while the function it returns
   * is run on the executor.
   *
   * @param driver - The class name of the JDBC driver for the given url.
   * @param url - The JDBC url to connect to.
   *
   * @return A function that loads the driver and connects to the url.
   */
  def getConnector(driver: String, url: String, properties: Properties): () => Connection = {
    () => {
      try {
<<<<<<< HEAD
        if (driver != null) Class.forName(driver)
=======
        if (driver != null) DriverRegistry.register(driver)
>>>>>>> upstream/master
      } catch {
        case e: ClassNotFoundException => {
          logWarning(s"Couldn't find class $driver", e);
        }
      }
      DriverManager.getConnection(url, properties)
    }
  }
<<<<<<< HEAD
=======

>>>>>>> upstream/master
  /**
   * Build and return JDBCRDD from the given information.
   *
   * @param sc - Your SparkContext.
   * @param schema - The Catalyst schema of the underlying database table.
   * @param driver - The class name of the JDBC driver for the given url.
   * @param url - The JDBC url to connect to.
   * @param fqTable - The fully-qualified table name (or paren'd SQL query) to use.
   * @param requiredColumns - The names of the columns to SELECT.
   * @param filters - The filters to include in all WHERE clauses.
   * @param parts - An array of JDBCPartitions specifying partition ids and
   *    per-partition WHERE clauses.
   *
   * @return An RDD representing "SELECT requiredColumns FROM fqTable".
   */
  def scanTable(
      sc: SparkContext,
      schema: StructType,
      driver: String,
      url: String,
      properties: Properties,
      fqTable: String,
      requiredColumns: Array[String],
      filters: Array[Filter],
      parts: Array[Partition]): RDD[Row] = {
<<<<<<< HEAD

    val prunedSchema = pruneSchema(schema, requiredColumns)

    return new
        JDBCRDD(
          sc,
          getConnector(driver, url, properties),
          prunedSchema,
          fqTable,
          requiredColumns,
          filters,
          parts)
=======
    new JDBCRDD(
      sc,
      getConnector(driver, url, properties),
      pruneSchema(schema, requiredColumns),
      fqTable,
      requiredColumns,
      filters,
      parts,
      properties)
>>>>>>> upstream/master
  }
}

/**
 * An RDD representing a table in a database accessed via JDBC.  Both the
 * driver code and the workers must be able to access the database; the driver
 * needs to fetch the schema while the workers need to fetch the data.
 */
private[sql] class JDBCRDD(
    sc: SparkContext,
    getConnection: () => Connection,
    schema: StructType,
    fqTable: String,
    columns: Array[String],
    filters: Array[Filter],
<<<<<<< HEAD
    partitions: Array[Partition])
=======
    partitions: Array[Partition],
    properties: Properties)
>>>>>>> upstream/master
  extends RDD[Row](sc, Nil) {

  /**
   * Retrieve the list of partitions corresponding to this RDD.
   */
  override def getPartitions: Array[Partition] = partitions

  /**
   * `columns`, but as a String suitable for injection into a SQL query.
   */
  private val columnList: String = {
    val sb = new StringBuilder()
    columns.foreach(x => sb.append(",").append(x))
    if (sb.length == 0) "1" else sb.substring(1)
  }

  /**
   * Converts value to SQL expression.
   */
  private def compileValue(value: Any): Any = value match {
    case stringValue: UTF8String => s"'${escapeSql(stringValue.toString)}'"
    case _ => value
  }

<<<<<<< HEAD
=======
  private def escapeSql(value: String): String =
    if (value == null) null else StringUtils.replace(value, "'", "''")

>>>>>>> upstream/master
  /**
   * Turns a single Filter into a String representing a SQL expression.
   * Returns null for an unhandled filter.
   */
  private def compileFilter(f: Filter): String = f match {
    case EqualTo(attr, value) => s"$attr = ${compileValue(value)}"
    case LessThan(attr, value) => s"$attr < ${compileValue(value)}"
    case GreaterThan(attr, value) => s"$attr > ${compileValue(value)}"
    case LessThanOrEqual(attr, value) => s"$attr <= ${compileValue(value)}"
    case GreaterThanOrEqual(attr, value) => s"$attr >= ${compileValue(value)}"
    case _ => null
  }

  /**
   * `filters`, but as a WHERE clause suitable for injection into a SQL query.
   */
  private val filterWhereClause: String = {
    val filterStrings = filters map compileFilter filter (_ != null)
    if (filterStrings.size > 0) {
      val sb = new StringBuilder("WHERE ")
      filterStrings.foreach(x => sb.append(x).append(" AND "))
      sb.substring(0, sb.length - 5)
    } else ""
  }

  /**
   * A WHERE clause representing both `filters`, if any, and the current partition.
   */
  private def getWhereClause(part: JDBCPartition): String = {
    if (part.whereClause != null && filterWhereClause.length > 0) {
      filterWhereClause + " AND " + part.whereClause
    } else if (part.whereClause != null) {
      "WHERE " + part.whereClause
    } else {
      filterWhereClause
    }
  }

  // Each JDBC-to-Catalyst conversion corresponds to a tag defined here so that
  // we don't have to potentially poke around in the Metadata once for every
<<<<<<< HEAD
  // row.  
=======
  // row.
>>>>>>> upstream/master
  // Is there a better way to do this?  I'd rather be using a type that
  // contains only the tags I define.
  abstract class JDBCConversion
  case object BooleanConversion extends JDBCConversion
  case object DateConversion extends JDBCConversion
<<<<<<< HEAD
  case object DecimalConversion extends JDBCConversion
=======
  case class  DecimalConversion(precisionInfo: Option[(Int, Int)]) extends JDBCConversion
>>>>>>> upstream/master
  case object DoubleConversion extends JDBCConversion
  case object FloatConversion extends JDBCConversion
  case object IntegerConversion extends JDBCConversion
  case object LongConversion extends JDBCConversion
  case object BinaryLongConversion extends JDBCConversion
  case object StringConversion extends JDBCConversion
  case object TimestampConversion extends JDBCConversion
  case object BinaryConversion extends JDBCConversion

  /**
   * Maps a StructType to a type tag list.
   */
  def getConversions(schema: StructType): Array[JDBCConversion] = {
    schema.fields.map(sf => sf.dataType match {
<<<<<<< HEAD
      case BooleanType           => BooleanConversion
      case DateType              => DateConversion
      case DecimalType.Unlimited => DecimalConversion
      case DoubleType            => DoubleConversion
      case FloatType             => FloatConversion
      case IntegerType           => IntegerConversion
      case LongType              =>
        if (sf.metadata.contains("binarylong")) BinaryLongConversion else LongConversion
      case StringType            => StringConversion
      case TimestampType         => TimestampConversion
      case BinaryType            => BinaryConversion
      case _                     => throw new IllegalArgumentException(s"Unsupported field $sf")
=======
      case BooleanType => BooleanConversion
      case DateType => DateConversion
      case DecimalType.Unlimited => DecimalConversion(None)
      case DecimalType.Fixed(d) => DecimalConversion(Some(d))
      case DoubleType => DoubleConversion
      case FloatType => FloatConversion
      case IntegerType => IntegerConversion
      case LongType =>
        if (sf.metadata.contains("binarylong")) BinaryLongConversion else LongConversion
      case StringType => StringConversion
      case TimestampType => TimestampConversion
      case BinaryType => BinaryConversion
      case _ => throw new IllegalArgumentException(s"Unsupported field $sf")
>>>>>>> upstream/master
    }).toArray
  }


  /**
   * Runs the SQL query against the JDBC driver.
   */
  override def compute(thePart: Partition, context: TaskContext): Iterator[Row] = new Iterator[Row]
  {
    var closed = false
    var finished = false
    var gotNext = false
    var nextValue: Row = null

    context.addTaskCompletionListener{ context => close() }
    val part = thePart.asInstanceOf[JDBCPartition]
    val conn = getConnection()

    // H2's JDBC driver does not support the setSchema() method.  We pass a
    // fully-qualified table name in the SELECT statement.  I don't know how to
    // talk about a table in a completely portable way.

    val myWhereClause = getWhereClause(part)

    val sqlText = s"SELECT $columnList FROM $fqTable $myWhereClause"
    val stmt = conn.prepareStatement(sqlText,
        ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
<<<<<<< HEAD
=======
    val fetchSize = properties.getProperty("fetchSize", "0").toInt
    stmt.setFetchSize(fetchSize)
>>>>>>> upstream/master
    val rs = stmt.executeQuery()

    val conversions = getConversions(schema)
    val mutableRow = new SpecificMutableRow(schema.fields.map(x => x.dataType))

    def getNext(): Row = {
      if (rs.next()) {
        var i = 0
        while (i < conversions.length) {
          val pos = i + 1
          conversions(i) match {
<<<<<<< HEAD
            case BooleanConversion    => mutableRow.setBoolean(i, rs.getBoolean(pos))
            // TODO(davies): convert Date into Int
            case DateConversion       => mutableRow.update(i, rs.getDate(pos))
            case DecimalConversion    => mutableRow.update(i, rs.getBigDecimal(pos))
            case DoubleConversion     => mutableRow.setDouble(i, rs.getDouble(pos))
            case FloatConversion      => mutableRow.setFloat(i, rs.getFloat(pos))
            case IntegerConversion    => mutableRow.setInt(i, rs.getInt(pos))
            case LongConversion       => mutableRow.setLong(i, rs.getLong(pos))
            // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
            case StringConversion     => mutableRow.setString(i, rs.getString(pos))
            case TimestampConversion  => mutableRow.update(i, rs.getTimestamp(pos))
            case BinaryConversion     => mutableRow.update(i, rs.getBytes(pos))
=======
            case BooleanConversion => mutableRow.setBoolean(i, rs.getBoolean(pos))
            case DateConversion =>
              // DateUtils.fromJavaDate does not handle null value, so we need to check it.
              val dateVal = rs.getDate(pos)
              if (dateVal != null) {
                mutableRow.update(i, DateUtils.fromJavaDate(dateVal))
              } else {
                mutableRow.update(i, null)
              }
            // When connecting with Oracle DB through JDBC, the precision and scale of BigDecimal
            // object returned by ResultSet.getBigDecimal is not correctly matched to the table
            // schema reported by ResultSetMetaData.getPrecision and ResultSetMetaData.getScale.
            // If inserting values like 19999 into a column with NUMBER(12, 2) type, you get through
            // a BigDecimal object with scale as 0. But the dataframe schema has correct type as
            // DecimalType(12, 2). Thus, after saving the dataframe into parquet file and then
            // retrieve it, you will get wrong result 199.99.
            // So it is needed to set precision and scale for Decimal based on JDBC metadata.
            case DecimalConversion(Some((p, s))) =>
              val decimalVal = rs.getBigDecimal(pos)
              if (decimalVal == null) {
                mutableRow.update(i, null)
              } else {
                mutableRow.update(i, Decimal(decimalVal, p, s))
              }
            case DecimalConversion(None) =>
              val decimalVal = rs.getBigDecimal(pos)
              if (decimalVal == null) {
                mutableRow.update(i, null)
              } else {
                mutableRow.update(i, Decimal(decimalVal))
              }
            case DoubleConversion => mutableRow.setDouble(i, rs.getDouble(pos))
            case FloatConversion => mutableRow.setFloat(i, rs.getFloat(pos))
            case IntegerConversion => mutableRow.setInt(i, rs.getInt(pos))
            case LongConversion => mutableRow.setLong(i, rs.getLong(pos))
            // TODO(davies): use getBytes for better performance, if the encoding is UTF-8
            case StringConversion => mutableRow.setString(i, rs.getString(pos))
            case TimestampConversion => mutableRow.update(i, rs.getTimestamp(pos))
            case BinaryConversion => mutableRow.update(i, rs.getBytes(pos))
>>>>>>> upstream/master
            case BinaryLongConversion => {
              val bytes = rs.getBytes(pos)
              var ans = 0L
              var j = 0
              while (j < bytes.size) {
                ans = 256 * ans + (255 & bytes(j))
                j = j + 1;
              }
              mutableRow.setLong(i, ans)
            }
          }
          if (rs.wasNull) mutableRow.setNullAt(i)
          i = i + 1
        }
        mutableRow
      } else {
        finished = true
        null.asInstanceOf[Row]
      }
    }

    def close() {
      if (closed) return
      try {
        if (null != rs) {
          rs.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing resultset", e)
      }
      try {
        if (null != stmt) {
          stmt.close()
        }
      } catch {
        case e: Exception => logWarning("Exception closing statement", e)
      }
      try {
        if (null != conn) {
          conn.close()
        }
        logInfo("closed connection")
      } catch {
        case e: Exception => logWarning("Exception closing connection", e)
      }
    }

    override def hasNext: Boolean = {
      if (!finished) {
        if (!gotNext) {
          nextValue = getNext()
          if (finished) {
            close()
          }
          gotNext = true
        }
      }
      !finished
    }

    override def next(): Row = {
      if (!hasNext) {
        throw new NoSuchElementException("End of stream")
      }
      gotNext = false
      nextValue
    }
  }
}
