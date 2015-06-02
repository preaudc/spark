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

package org.apache.spark.sql.hive

<<<<<<< HEAD
import java.io.IOException
import java.util.{List => JList}

import com.google.common.base.Objects
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hadoop.hive.metastore.api.{FieldSchema, Partition => TPartition, Table => TTable}
import org.apache.hadoop.hive.metastore.{TableType, Warehouse}
import org.apache.hadoop.hive.ql.metadata._
import org.apache.hadoop.hive.ql.plan.CreateTableDesc
import org.apache.hadoop.hive.serde.serdeConstants
import org.apache.hadoop.hive.serde2.`lazy`.LazySimpleSerDe
import org.apache.hadoop.hive.serde2.{Deserializer, SerDeException}
import org.apache.hadoop.util.ReflectionUtils

import org.apache.spark.Logging
import org.apache.spark.sql.{SaveMode, AnalysisException, SQLContext}
import org.apache.spark.sql.catalyst.analysis.{MultiInstanceRelation, NoSuchTableException, Catalog, OverrideCatalog}
=======
import com.google.common.base.Objects
import com.google.common.cache.{CacheBuilder, CacheLoader, LoadingCache}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hive.metastore.Warehouse
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.ql.metadata._
import org.apache.hadoop.hive.serde2.Deserializer

import org.apache.spark.Logging
import org.apache.spark.sql.catalyst.analysis.{Catalog, MultiInstanceRelation, OverrideCatalog}
>>>>>>> upstream/master
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
<<<<<<< HEAD
import org.apache.spark.sql.parquet.{ParquetRelation2, Partition => ParquetPartition, PartitionSpec}
import org.apache.spark.sql.sources.{CreateTableUsingAsSelect, DDLParser, LogicalRelation, ResolvedDataSource}
import org.apache.spark.sql.types._
=======
import org.apache.spark.sql.hive.client._
import org.apache.spark.sql.parquet.ParquetRelation2
import org.apache.spark.sql.sources.{CreateTableUsingAsSelect, LogicalRelation, Partition => ParquetPartition, PartitionSpec, ResolvedDataSource}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, SQLContext, SaveMode, sources}
>>>>>>> upstream/master
import org.apache.spark.util.Utils

/* Implicit conversions */
import scala.collection.JavaConversions._

private[hive] class HiveMetastoreCatalog(val client: ClientInterface, hive: HiveContext)
  extends Catalog with Logging {

  val conf = hive.conf

  /** Usages should lock on `this`. */
  protected[hive] lazy val hiveWarehouse = new Warehouse(hive.hiveconf)
<<<<<<< HEAD

  // TODO: Use this everywhere instead of tuples or databaseName, tableName,.
  /** A fully qualified identifier for a table (i.e., database.tableName) */
  case class QualifiedTableName(database: String, name: String) {
    def toLowerCase: QualifiedTableName = QualifiedTableName(database.toLowerCase, name.toLowerCase)
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  protected[hive] val cachedDataSourceTables: LoadingCache[QualifiedTableName, LogicalPlan] = {
    val cacheLoader = new CacheLoader[QualifiedTableName, LogicalPlan]() {
      override def load(in: QualifiedTableName): LogicalPlan = {
        logDebug(s"Creating new cached data source for $in")
        val table = HiveMetastoreCatalog.this.synchronized {
          client.getTable(in.database, in.name)
        }

        def schemaStringFromParts: Option[String] = {
          Option(table.getProperty("spark.sql.sources.schema.numParts")).map { numParts =>
            val parts = (0 until numParts.toInt).map { index =>
              val part = table.getProperty(s"spark.sql.sources.schema.part.${index}")
              if (part == null) {
                throw new AnalysisException(
                  s"Could not read schema from the metastore because it is corrupted " +
                  s"(missing part ${index} of the schema).")
              }

              part
            }
            // Stick all parts back to a single schema string.
            parts.mkString
          }
        }

        // Originally, we used spark.sql.sources.schema to store the schema of a data source table.
        // After SPARK-6024, we removed this flag.
        // Although we are not using spark.sql.sources.schema any more, we need to still support.
        val schemaString =
          Option(table.getProperty("spark.sql.sources.schema")).orElse(schemaStringFromParts)

        val userSpecifiedSchema =
          schemaString.map(s => DataType.fromJson(s).asInstanceOf[StructType])

        // It does not appear that the ql client for the metastore has a way to enumerate all the
        // SerDe properties directly...
        val options = table.getTTable.getSd.getSerdeInfo.getParameters.toMap

        val resolvedRelation =
          ResolvedDataSource(
            hive,
            userSpecifiedSchema,
            table.getProperty("spark.sql.sources.provider"),
            options)

        LogicalRelation(resolvedRelation.relation)
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

  override def refreshTable(databaseName: String, tableName: String): Unit = {
    // refreshTable does not eagerly reload the cache. It just invalidate the cache.
    // Next time when we use the table, it will be populated in the cache.
    // Since we also cache ParquetRealtions converted from Hive Parquet tables and
    // adding converted ParquetRealtions into the cache is not defined in the load function
    // of the cache (instead, we add the cache entry in convertToParquetRelation),
    // it is better at here to invalidate the cache to avoid confusing waring logs from the
    // cache loader (e.g. cannot find data source provider, which is only defined for
    // data source table.).
    invalidateTable(databaseName, tableName)
  }

  def invalidateTable(databaseName: String, tableName: String): Unit = {
    cachedDataSourceTables.invalidate(QualifiedTableName(databaseName, tableName).toLowerCase)
  }

  val caseSensitive: Boolean = false

  /**
   * Creates a data source table (a table created with USING clause) in Hive's metastore.
   * Returns true when the table has been created. Otherwise, false.
   */
  def createDataSourceTable(
      tableName: String,
      userSpecifiedSchema: Option[StructType],
      provider: String,
      options: Map[String, String],
      isExternal: Boolean): Unit = {
    val (dbName, tblName) = processDatabaseAndTableName("default", tableName)
    val tbl = new Table(dbName, tblName)

    tbl.setProperty("spark.sql.sources.provider", provider)
    if (userSpecifiedSchema.isDefined) {
      val threshold = hive.conf.schemaStringLengthThreshold
      val schemaJsonString = userSpecifiedSchema.get.json
      // Split the JSON string.
      val parts = schemaJsonString.grouped(threshold).toSeq
      tbl.setProperty("spark.sql.sources.schema.numParts", parts.size.toString)
      parts.zipWithIndex.foreach { case (part, index) =>
        tbl.setProperty(s"spark.sql.sources.schema.part.${index}", part)
      }
    }
    options.foreach { case (key, value) => tbl.setSerdeParam(key, value) }

    if (isExternal) {
      tbl.setProperty("EXTERNAL", "TRUE")
      tbl.setTableType(TableType.EXTERNAL_TABLE)
    } else {
      tbl.setProperty("EXTERNAL", "FALSE")
      tbl.setTableType(TableType.MANAGED_TABLE)
    }

    // create the table
    synchronized {
      client.createTable(tbl, false)
    }
  }

  def hiveDefaultTableFilePath(tableName: String): String = synchronized {
    val currentDatabase = client.getDatabase(hive.sessionState.getCurrentDatabase)

    hiveWarehouse.getTablePath(currentDatabase, tableName).toString
  }

  def tableExists(tableIdentifier: Seq[String]): Boolean = synchronized {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val databaseName =
      tableIdent
        .lift(tableIdent.size - 2)
        .getOrElse(hive.sessionState.getCurrentDatabase)
    val tblName = tableIdent.last
    client.getTable(databaseName, tblName, false) != null
  }

  def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String]): LogicalPlan = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val databaseName = tableIdent.lift(tableIdent.size - 2).getOrElse(
      hive.sessionState.getCurrentDatabase)
    val tblName = tableIdent.last
    val table = try {
      synchronized {
        client.getTable(databaseName, tblName)
      }
    } catch {
      case te: org.apache.hadoop.hive.ql.metadata.InvalidTableException =>
        throw new NoSuchTableException
    }

    if (table.getProperty("spark.sql.sources.provider") != null) {
      val dataSourceTable =
        cachedDataSourceTables(QualifiedTableName(databaseName, tblName).toLowerCase)
      // Then, if alias is specified, wrap the table with a Subquery using the alias.
      // Othersie, wrap the table with a Subquery using the table name.
      val withAlias =
        alias.map(a => Subquery(a, dataSourceTable)).getOrElse(
          Subquery(tableIdent.last, dataSourceTable))

      withAlias
    } else if (table.isView) {
      // if the unresolved relation is from hive view
      // parse the text into logic node.
      HiveQl.createPlanForView(table, alias)
    } else {
      val partitions: Seq[Partition] =
        if (table.isPartitioned) {
          synchronized {
            HiveShim.getAllPartitionsOf(client, table).toSeq
          }
        } else {
          Nil
        }

      MetastoreRelation(databaseName, tblName, alias)(
        table.getTTable, partitions.map(part => part.getTPartition))(hive)
    }
  }

  private def convertToParquetRelation(metastoreRelation: MetastoreRelation): LogicalRelation = {
    val metastoreSchema = StructType.fromAttributes(metastoreRelation.output)
    val mergeSchema = hive.convertMetastoreParquetWithSchemaMerging

    // NOTE: Instead of passing Metastore schema directly to `ParquetRelation2`, we have to
    // serialize the Metastore schema to JSON and pass it as a data source option because of the
    // evil case insensitivity issue, which is reconciled within `ParquetRelation2`.
    val parquetOptions = Map(
      ParquetRelation2.METASTORE_SCHEMA -> metastoreSchema.json,
      ParquetRelation2.MERGE_SCHEMA -> mergeSchema.toString)
    val tableIdentifier =
      QualifiedTableName(metastoreRelation.databaseName, metastoreRelation.tableName)

    def getCached(
        tableIdentifier: QualifiedTableName,
        pathsInMetastore: Seq[String],
        schemaInMetastore: StructType,
        partitionSpecInMetastore: Option[PartitionSpec]): Option[LogicalRelation] = {
      cachedDataSourceTables.getIfPresent(tableIdentifier) match {
        case null => None // Cache miss
        case logical@LogicalRelation(parquetRelation: ParquetRelation2) =>
          // If we have the same paths, same schema, and same partition spec,
          // we will use the cached Parquet Relation.
          val useCached =
            parquetRelation.paths.toSet == pathsInMetastore.toSet &&
            logical.schema.sameType(metastoreSchema) &&
            parquetRelation.maybePartitionSpec == partitionSpecInMetastore

          if (useCached) {
            Some(logical)
          } else {
            // If the cached relation is not updated, we invalidate it right away.
            cachedDataSourceTables.invalidate(tableIdentifier)
            None
          }
        case other =>
          logWarning(
            s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} should be stored " +
              s"as Parquet. However, we are getting a ${other} from the metastore cache. " +
              s"This cached entry will be invalidated.")
          cachedDataSourceTables.invalidate(tableIdentifier)
          None
      }
    }

    val result = if (metastoreRelation.hiveQlTable.isPartitioned) {
      val partitionSchema = StructType.fromAttributes(metastoreRelation.partitionKeys)
      val partitionColumnDataTypes = partitionSchema.map(_.dataType)
      val partitions = metastoreRelation.hiveQlPartitions.map { p =>
        val location = p.getLocation
        val values = Row.fromSeq(p.getValues.zip(partitionColumnDataTypes).map {
          case (rawValue, dataType) => Cast(Literal(rawValue), dataType).eval(null)
        })
        ParquetPartition(values, location)
      }
      val partitionSpec = PartitionSpec(partitionSchema, partitions)
      val paths = partitions.map(_.path)

      val cached = getCached(tableIdentifier, paths, metastoreSchema, Some(partitionSpec))
      val parquetRelation = cached.getOrElse {
        val created =
          LogicalRelation(ParquetRelation2(paths, parquetOptions, None, Some(partitionSpec))(hive))
        cachedDataSourceTables.put(tableIdentifier, created)
        created
      }

      parquetRelation
    } else {
      val paths = Seq(metastoreRelation.hiveQlTable.getDataLocation.toString)

      val cached = getCached(tableIdentifier, paths, metastoreSchema, None)
      val parquetRelation = cached.getOrElse {
        val created =
          LogicalRelation(ParquetRelation2(paths, parquetOptions)(hive))
        cachedDataSourceTables.put(tableIdentifier, created)
        created
=======

  // TODO: Use this everywhere instead of tuples or databaseName, tableName,.
  /** A fully qualified identifier for a table (i.e., database.tableName) */
  case class QualifiedTableName(database: String, name: String) {
    def toLowerCase: QualifiedTableName = QualifiedTableName(database.toLowerCase, name.toLowerCase)
  }

  /** A cache of Spark SQL data source tables that have been accessed. */
  protected[hive] val cachedDataSourceTables: LoadingCache[QualifiedTableName, LogicalPlan] = {
    val cacheLoader = new CacheLoader[QualifiedTableName, LogicalPlan]() {
      override def load(in: QualifiedTableName): LogicalPlan = {
        logDebug(s"Creating new cached data source for $in")
        val table = client.getTable(in.database, in.name)

        def schemaStringFromParts: Option[String] = {
          table.properties.get("spark.sql.sources.schema.numParts").map { numParts =>
            val parts = (0 until numParts.toInt).map { index =>
              val part = table.properties.get(s"spark.sql.sources.schema.part.$index").orNull
              if (part == null) {
                throw new AnalysisException(
                  "Could not read schema from the metastore because it is corrupted " +
                    s"(missing part $index of the schema, $numParts parts are expected).")
              }

              part
            }
            // Stick all parts back to a single schema string.
            parts.mkString
          }
        }

        // Originally, we used spark.sql.sources.schema to store the schema of a data source table.
        // After SPARK-6024, we removed this flag.
        // Although we are not using spark.sql.sources.schema any more, we need to still support.
        val schemaString =
          table.properties.get("spark.sql.sources.schema").orElse(schemaStringFromParts)

        val userSpecifiedSchema =
          schemaString.map(s => DataType.fromJson(s).asInstanceOf[StructType])

        // We only need names at here since userSpecifiedSchema we loaded from the metastore
        // contains partition columns. We can always get datatypes of partitioning columns
        // from userSpecifiedSchema.
        val partitionColumns = table.partitionColumns.map(_.name)

        // It does not appear that the ql client for the metastore has a way to enumerate all the
        // SerDe properties directly...
        val options = table.serdeProperties

        val resolvedRelation =
          ResolvedDataSource(
            hive,
            userSpecifiedSchema,
            partitionColumns.toArray,
            table.properties("spark.sql.sources.provider"),
            options)

        LogicalRelation(resolvedRelation.relation)
>>>>>>> upstream/master
      }
    }

    CacheBuilder.newBuilder().maximumSize(1000).build(cacheLoader)
  }

<<<<<<< HEAD
      parquetRelation
    }

    result.newInstance()
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = synchronized {
    val dbName = if (!caseSensitive) {
      if (databaseName.isDefined) Some(databaseName.get.toLowerCase) else None
    } else {
      databaseName
    }
    val db = dbName.getOrElse(hive.sessionState.getCurrentDatabase)

    client.getAllTables(db).map(tableName => (tableName, false))
=======
  override def refreshTable(databaseName: String, tableName: String): Unit = {
    // refreshTable does not eagerly reload the cache. It just invalidate the cache.
    // Next time when we use the table, it will be populated in the cache.
    // Since we also cache ParquetRelations converted from Hive Parquet tables and
    // adding converted ParquetRelations into the cache is not defined in the load function
    // of the cache (instead, we add the cache entry in convertToParquetRelation),
    // it is better at here to invalidate the cache to avoid confusing waring logs from the
    // cache loader (e.g. cannot find data source provider, which is only defined for
    // data source table.).
    invalidateTable(databaseName, tableName)
>>>>>>> upstream/master
  }

  def invalidateTable(databaseName: String, tableName: String): Unit = {
    cachedDataSourceTables.invalidate(QualifiedTableName(databaseName, tableName).toLowerCase)
  }

  val caseSensitive: Boolean = false

  /**
   * Creates a data source table (a table created with USING clause) in Hive's metastore.
   * Returns true when the table has been created. Otherwise, false.
   */
  def createDataSourceTable(
      tableName: String,
      userSpecifiedSchema: Option[StructType],
      partitionColumns: Array[String],
      provider: String,
      options: Map[String, String],
      isExternal: Boolean): Unit = {
    val (dbName, tblName) = processDatabaseAndTableName("default", tableName)
    val tableProperties = new scala.collection.mutable.HashMap[String, String]
    tableProperties.put("spark.sql.sources.provider", provider)

    // Saves optional user specified schema.  Serialized JSON schema string may be too long to be
    // stored into a single metastore SerDe property.  In this case, we split the JSON string and
    // store each part as a separate SerDe property.
    if (userSpecifiedSchema.isDefined) {
      val threshold = conf.schemaStringLengthThreshold
      val schemaJsonString = userSpecifiedSchema.get.json
      // Split the JSON string.
      val parts = schemaJsonString.grouped(threshold).toSeq
      tableProperties.put("spark.sql.sources.schema.numParts", parts.size.toString)
      parts.zipWithIndex.foreach { case (part, index) =>
        tableProperties.put(s"spark.sql.sources.schema.part.$index", part)
      }
    }

    val metastorePartitionColumns = userSpecifiedSchema.map { schema =>
      val fields = partitionColumns.map(col => schema(col))
      fields.map { field =>
        HiveColumn(
          name = field.name,
          hiveType = HiveMetastoreTypes.toMetastoreType(field.dataType),
          comment = "")
      }.toSeq
    }.getOrElse {
      if (partitionColumns.length > 0) {
        // The table does not have a specified schema, which means that the schema will be inferred
        // when we load the table. So, we are not expecting partition columns and we will discover
        // partitions when we load the table. However, if there are specified partition columns,
        // we simplily ignore them and provide a warning message..
        logWarning(
          s"The schema and partitions of table $tableName will be inferred when it is loaded. " +
            s"Specified partition columns (${partitionColumns.mkString(",")}) will be ignored.")
      }
      Seq.empty[HiveColumn]
    }

    val tableType = if (isExternal) {
      tableProperties.put("EXTERNAL", "TRUE")
      ExternalTable
    } else {
<<<<<<< HEAD
      schema.map(attr => new FieldSchema(attr.name, toMetastoreType(attr.dataType), null))
=======
      tableProperties.put("EXTERNAL", "FALSE")
      ManagedTable
>>>>>>> upstream/master
    }

    client.createTable(
      HiveTable(
        specifiedDatabase = Option(dbName),
        name = tblName,
        schema = Seq.empty,
        partitionColumns = metastorePartitionColumns,
        tableType = tableType,
        properties = tableProperties.toMap,
        serdeProperties = options))
  }

  def hiveDefaultTableFilePath(tableName: String): String = {
    // Code based on: hiveWarehouse.getTablePath(currentDatabase, tableName)
    new Path(
      new Path(client.getDatabase(client.currentDatabase).location),
      tableName.toLowerCase).toString
  }

  def tableExists(tableIdentifier: Seq[String]): Boolean = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val databaseName =
      tableIdent
        .lift(tableIdent.size - 2)
        .getOrElse(client.currentDatabase)
    val tblName = tableIdent.last
    client.getTableOption(databaseName, tblName).isDefined
  }

<<<<<<< HEAD
    /*
     * We use LazySimpleSerDe by default.
     *
     * If the user didn't specify a SerDe, and any of the columns are not simple
     * types, we will have to use DynamicSerDe instead.
     */
    if (crtTbl == null || crtTbl.getSerName() == null) {
      val storageHandler = tbl.getStorageHandler()
      if (storageHandler == null) {
        logInfo(s"Default to LazySimpleSerDe for table $dbName.$tblName")
        tbl.setSerializationLib(classOf[LazySimpleSerDe].getName())

        import org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat
        import org.apache.hadoop.io.Text
        import org.apache.hadoop.mapred.TextInputFormat

        tbl.setInputFormatClass(classOf[TextInputFormat])
        tbl.setOutputFormatClass(classOf[HiveIgnoreKeyTextOutputFormat[Text, Text]])
        tbl.setSerializationLib("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe")
      } else {
        val serDeClassName = storageHandler.getSerDeClass().getName()
        logInfo(s"Use StorageHandler-supplied $serDeClassName for table $dbName.$tblName")
        tbl.setSerializationLib(serDeClassName)
=======
  def lookupRelation(
      tableIdentifier: Seq[String],
      alias: Option[String]): LogicalPlan = {
    val tableIdent = processTableIdentifier(tableIdentifier)
    val databaseName = tableIdent.lift(tableIdent.size - 2).getOrElse(
      client.currentDatabase)
    val tblName = tableIdent.last
    val table = client.getTable(databaseName, tblName)

    if (table.properties.get("spark.sql.sources.provider").isDefined) {
      val dataSourceTable =
        cachedDataSourceTables(QualifiedTableName(databaseName, tblName).toLowerCase)
      // Then, if alias is specified, wrap the table with a Subquery using the alias.
      // Otherwise, wrap the table with a Subquery using the table name.
      val withAlias =
        alias.map(a => Subquery(a, dataSourceTable)).getOrElse(
          Subquery(tableIdent.last, dataSourceTable))

      withAlias
    } else if (table.tableType == VirtualView) {
      val viewText = table.viewText.getOrElse(sys.error("Invalid view without text."))
      alias match {
        // because hive use things like `_c0` to build the expanded text
        // currently we cannot support view from "create view v1(c1) as ..."
        case None => Subquery(table.name, HiveQl.createPlan(viewText))
        case Some(aliasText) => Subquery(aliasText, HiveQl.createPlan(viewText))
>>>>>>> upstream/master
      }
    } else {
      MetastoreRelation(databaseName, tblName, alias)(table)(hive)
    }
  }

  private def convertToParquetRelation(metastoreRelation: MetastoreRelation): LogicalRelation = {
    val metastoreSchema = StructType.fromAttributes(metastoreRelation.output)
    val mergeSchema = hive.convertMetastoreParquetWithSchemaMerging

    // NOTE: Instead of passing Metastore schema directly to `ParquetRelation2`, we have to
    // serialize the Metastore schema to JSON and pass it as a data source option because of the
    // evil case insensitivity issue, which is reconciled within `ParquetRelation2`.
    val parquetOptions = Map(
      ParquetRelation2.METASTORE_SCHEMA -> metastoreSchema.json,
      ParquetRelation2.MERGE_SCHEMA -> mergeSchema.toString)
    val tableIdentifier =
      QualifiedTableName(metastoreRelation.databaseName, metastoreRelation.tableName)

    def getCached(
        tableIdentifier: QualifiedTableName,
        pathsInMetastore: Seq[String],
        schemaInMetastore: StructType,
        partitionSpecInMetastore: Option[PartitionSpec]): Option[LogicalRelation] = {
      cachedDataSourceTables.getIfPresent(tableIdentifier) match {
        case null => None // Cache miss
        case logical@LogicalRelation(parquetRelation: ParquetRelation2) =>
          // If we have the same paths, same schema, and same partition spec,
          // we will use the cached Parquet Relation.
          val useCached =
            parquetRelation.paths.toSet == pathsInMetastore.toSet &&
            logical.schema.sameType(metastoreSchema) &&
            parquetRelation.partitionSpec == partitionSpecInMetastore.getOrElse {
              PartitionSpec(StructType(Nil), Array.empty[sources.Partition])
            }

          if (useCached) {
            Some(logical)
          } else {
            // If the cached relation is not updated, we invalidate it right away.
            cachedDataSourceTables.invalidate(tableIdentifier)
            None
          }
        case other =>
          logWarning(
            s"${metastoreRelation.databaseName}.${metastoreRelation.tableName} should be stored " +
              s"as Parquet. However, we are getting a $other from the metastore cache. " +
              s"This cached entry will be invalidated.")
          cachedDataSourceTables.invalidate(tableIdentifier)
          None
      }
    }
    HiveShim.setTblNullFormat(crtTbl, tbl)

    val result = if (metastoreRelation.hiveQlTable.isPartitioned) {
      val partitionSchema = StructType.fromAttributes(metastoreRelation.partitionKeys)
      val partitionColumnDataTypes = partitionSchema.map(_.dataType)
      val partitions = metastoreRelation.hiveQlPartitions.map { p =>
        val location = p.getLocation
        val values = Row.fromSeq(p.getValues.zip(partitionColumnDataTypes).map {
          case (rawValue, dataType) => Cast(Literal(rawValue), dataType).eval(null)
        })
        ParquetPartition(values, location)
      }
      val partitionSpec = PartitionSpec(partitionSchema, partitions)
      val paths = partitions.map(_.path)

      val cached = getCached(tableIdentifier, paths, metastoreSchema, Some(partitionSpec))
      val parquetRelation = cached.getOrElse {
        val created = LogicalRelation(
          new ParquetRelation2(
            paths.toArray, None, Some(partitionSpec), parquetOptions)(hive))
        cachedDataSourceTables.put(tableIdentifier, created)
        created
      }

      parquetRelation
    } else {
      val paths = Seq(metastoreRelation.hiveQlTable.getDataLocation.toString)

      val cached = getCached(tableIdentifier, paths, metastoreSchema, None)
      val parquetRelation = cached.getOrElse {
        val created = LogicalRelation(
          new ParquetRelation2(paths.toArray, None, None, parquetOptions)(hive))
        cachedDataSourceTables.put(tableIdentifier, created)
        created
      }

      parquetRelation
    }

    result.newInstance()
  }

  override def getTables(databaseName: Option[String]): Seq[(String, Boolean)] = {
    val db = databaseName.getOrElse(client.currentDatabase)

    client.listTables(db).map(tableName => (tableName, false))
  }

  protected def processDatabaseAndTableName(
      databaseName: Option[String],
      tableName: String): (Option[String], String) = {
    if (!caseSensitive) {
      (databaseName.map(_.toLowerCase), tableName.toLowerCase)
    } else {
      (databaseName, tableName)
    }
  }

  protected def processDatabaseAndTableName(
      databaseName: String,
      tableName: String): (String, String) = {
    if (!caseSensitive) {
      (databaseName.toLowerCase, tableName.toLowerCase)
    } else {
      (databaseName, tableName)
    }
  }

  /**
   * When scanning or writing to non-partitioned Metastore Parquet tables, convert them to Parquet
   * data source relations for better performance.
   *
   * This rule can be considered as [[HiveStrategies.ParquetConversion]] done right.
   */
  object ParquetConversions extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (!plan.resolved) {
        return plan
      }

      // Collects all `MetastoreRelation`s which should be replaced
      val toBeReplaced = plan.collect {
        // Write path
        case InsertIntoTable(relation: MetastoreRelation, _, _, _, _)
            // Inserting into partitioned table is not supported in Parquet data source (yet).
            if !relation.hiveQlTable.isPartitioned &&
              hive.convertMetastoreParquet &&
              conf.parquetUseDataSourceApi &&
              relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") =>
          val parquetRelation = convertToParquetRelation(relation)
          val attributedRewrites = relation.output.zip(parquetRelation.output)
          (relation, parquetRelation, attributedRewrites)

        // Write path
        case InsertIntoHiveTable(relation: MetastoreRelation, _, _, _, _)
          // Inserting into partitioned table is not supported in Parquet data source (yet).
          if !relation.hiveQlTable.isPartitioned &&
            hive.convertMetastoreParquet &&
            conf.parquetUseDataSourceApi &&
            relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") =>
          val parquetRelation = convertToParquetRelation(relation)
          val attributedRewrites = relation.output.zip(parquetRelation.output)
          (relation, parquetRelation, attributedRewrites)

        // Read path
        case p @ PhysicalOperation(_, _, relation: MetastoreRelation)
            if hive.convertMetastoreParquet &&
              conf.parquetUseDataSourceApi &&
              relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") =>
          val parquetRelation = convertToParquetRelation(relation)
          val attributedRewrites = relation.output.zip(parquetRelation.output)
          (relation, parquetRelation, attributedRewrites)
      }

      val relationMap = toBeReplaced.map(r => (r._1, r._2)).toMap
      val attributedRewrites = AttributeMap(toBeReplaced.map(_._3).fold(Nil)(_ ++: _))

      // Replaces all `MetastoreRelation`s with corresponding `ParquetRelation2`s, and fixes
      // attribute IDs referenced in other nodes.
      plan.transformUp {
        case r: MetastoreRelation if relationMap.contains(r) =>
          val parquetRelation = relationMap(r)
          val alias = r.alias.getOrElse(r.tableName)
          Subquery(alias, parquetRelation)

        case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          if relationMap.contains(r) =>
          val parquetRelation = relationMap(r)
          InsertIntoTable(parquetRelation, partition, child, overwrite, ifNotExists)

        case InsertIntoHiveTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          if relationMap.contains(r) =>
          val parquetRelation = relationMap(r)
          InsertIntoTable(parquetRelation, partition, child, overwrite, ifNotExists)

        case other => other.transformExpressions {
          case a: Attribute if a.resolved => attributedRewrites.getOrElse(a, a)
        }
      }
    }
  }

  protected def processDatabaseAndTableName(
      databaseName: Option[String],
      tableName: String): (Option[String], String) = {
    if (!caseSensitive) {
      (databaseName.map(_.toLowerCase), tableName.toLowerCase)
    } else {
      (databaseName, tableName)
    }
  }

  protected def processDatabaseAndTableName(
      databaseName: String,
      tableName: String): (String, String) = {
    if (!caseSensitive) {
      (databaseName.toLowerCase, tableName.toLowerCase)
    } else {
      (databaseName, tableName)
    }
  }

  /**
   * When scanning or writing to non-partitioned Metastore Parquet tables, convert them to Parquet
   * data source relations for better performance.
   *
   * This rule can be considered as [[HiveStrategies.ParquetConversion]] done right.
   */
  object ParquetConversions extends Rule[LogicalPlan] {
    override def apply(plan: LogicalPlan): LogicalPlan = {
      if (!plan.resolved) {
        return plan
      }

      // Collects all `MetastoreRelation`s which should be replaced
      val toBeReplaced = plan.collect {
        // Write path
        case InsertIntoTable(relation: MetastoreRelation, _, _, _, _)
            // Inserting into partitioned table is not supported in Parquet data source (yet).
            if !relation.hiveQlTable.isPartitioned &&
              hive.convertMetastoreParquet &&
              hive.conf.parquetUseDataSourceApi &&
              relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") =>
          val parquetRelation = convertToParquetRelation(relation)
          val attributedRewrites = relation.output.zip(parquetRelation.output)
          (relation, parquetRelation, attributedRewrites)

        // Write path
        case InsertIntoHiveTable(relation: MetastoreRelation, _, _, _, _)
          // Inserting into partitioned table is not supported in Parquet data source (yet).
          if !relation.hiveQlTable.isPartitioned &&
            hive.convertMetastoreParquet &&
            hive.conf.parquetUseDataSourceApi &&
            relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") =>
          val parquetRelation = convertToParquetRelation(relation)
          val attributedRewrites = relation.output.zip(parquetRelation.output)
          (relation, parquetRelation, attributedRewrites)

        // Read path
        case p @ PhysicalOperation(_, _, relation: MetastoreRelation)
            if hive.convertMetastoreParquet &&
              hive.conf.parquetUseDataSourceApi &&
              relation.tableDesc.getSerdeClassName.toLowerCase.contains("parquet") =>
          val parquetRelation = convertToParquetRelation(relation)
          val attributedRewrites = relation.output.zip(parquetRelation.output)
          (relation, parquetRelation, attributedRewrites)
      }

      val relationMap = toBeReplaced.map(r => (r._1, r._2)).toMap
      val attributedRewrites = AttributeMap(toBeReplaced.map(_._3).fold(Nil)(_ ++: _))

      // Replaces all `MetastoreRelation`s with corresponding `ParquetRelation2`s, and fixes
      // attribute IDs referenced in other nodes.
      plan.transformUp {
        case r: MetastoreRelation if relationMap.contains(r) =>
          val parquetRelation = relationMap(r)
          val alias = r.alias.getOrElse(r.tableName)
          Subquery(alias, parquetRelation)

        case InsertIntoTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          if relationMap.contains(r) =>
          val parquetRelation = relationMap(r)
          InsertIntoTable(parquetRelation, partition, child, overwrite, ifNotExists)

        case InsertIntoHiveTable(r: MetastoreRelation, partition, child, overwrite, ifNotExists)
          if relationMap.contains(r) =>
          val parquetRelation = relationMap(r)
          InsertIntoTable(parquetRelation, partition, child, overwrite, ifNotExists)

        case other => other.transformExpressions {
          case a: Attribute if a.resolved => attributedRewrites.getOrElse(a, a)
        }
      }
    }
  }

  /**
   * Creates any tables required for query execution.
   * For example, because of a CREATE TABLE X AS statement.
   */
  object CreateTables extends Rule[LogicalPlan] {
    import org.apache.hadoop.hive.ql.Context
    import org.apache.hadoop.hive.ql.parse.{ASTNode, QB, SemanticAnalyzer}

    def apply(plan: LogicalPlan): LogicalPlan = plan transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p
      case p: LogicalPlan if p.resolved => p
      case p @ CreateTableAsSelect(table, child, allowExisting) =>
        val schema = if (table.schema.size > 0) {
          table.schema
        } else {
          child.output.map {
            attr => new HiveColumn(
              attr.name,
              HiveMetastoreTypes.toMetastoreType(attr.dataType), null)
          }
        }

<<<<<<< HEAD
      // TODO extra is in type of ASTNode which means the logical plan is not resolved
      // Need to think about how to implement the CreateTableAsSelect.resolved
      case CreateTableAsSelect(db, tableName, child, allowExisting, Some(extra: ASTNode)) =>
        val (dbName, tblName) = processDatabaseAndTableName(db, tableName)
        val databaseName = dbName.getOrElse(hive.sessionState.getCurrentDatabase)

        // Get the CreateTableDesc from Hive SemanticAnalyzer
        val desc: Option[CreateTableDesc] = if (tableExists(Seq(databaseName, tblName))) {
          None
        } else {
          val sa = new SemanticAnalyzer(hive.hiveconf) {
            override def analyzeInternal(ast: ASTNode) {
              // A hack to intercept the SemanticAnalyzer.analyzeInternal,
              // to ignore the SELECT clause of the CTAS
              val method = classOf[SemanticAnalyzer].getDeclaredMethod(
                "analyzeCreateTable", classOf[ASTNode], classOf[QB])
              method.setAccessible(true)
              method.invoke(this, ast, this.getQB)
            }
          }

          sa.analyze(extra, new Context(hive.hiveconf))
          Some(sa.getQB().getTableDesc)
        }

        // Check if the query specifies file format or storage handler.
        val hasStorageSpec = desc match {
          case Some(crtTbl) =>
            crtTbl != null && (crtTbl.getSerName != null || crtTbl.getStorageHandler != null)
          case None => false
        }

        if (hive.convertCTAS && !hasStorageSpec) {
          // Do the conversion when spark.sql.hive.convertCTAS is true and the query
          // does not specify any storage format (file format and storage handler).
          if (dbName.isDefined) {
            throw new AnalysisException(
              "Cannot specify database name in a CTAS statement " +
              "when spark.sql.hive.convertCTAS is set to true.")
=======
        val desc = table.copy(schema = schema)

        if (hive.convertCTAS && table.serde.isEmpty) {
          // Do the conversion when spark.sql.hive.convertCTAS is true and the query
          // does not specify any storage format (file format and storage handler).
          if (table.specifiedDatabase.isDefined) {
            throw new AnalysisException(
              "Cannot specify database name in a CTAS statement " +
                "when spark.sql.hive.convertCTAS is set to true.")
>>>>>>> upstream/master
          }

          val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
          CreateTableUsingAsSelect(
<<<<<<< HEAD
            tblName,
            hive.conf.defaultDataSourceName,
            temporary = false,
=======
            desc.name,
            hive.conf.defaultDataSourceName,
            temporary = false,
            Array.empty[String],
>>>>>>> upstream/master
            mode,
            options = Map.empty[String, String],
            child
          )
        } else {
<<<<<<< HEAD
          execution.CreateTableAsSelect(
            databaseName,
            tableName,
            child,
            allowExisting,
            desc)
        }

      case p: LogicalPlan if p.resolved => p

      case p @ CreateTableAsSelect(db, tableName, child, allowExisting, None) =>
        val (dbName, tblName) = processDatabaseAndTableName(db, tableName)
        if (hive.convertCTAS) {
          if (dbName.isDefined) {
            throw new AnalysisException(
              "Cannot specify database name in a CTAS statement " +
              "when spark.sql.hive.convertCTAS is set to true.")
          }

          val mode = if (allowExisting) SaveMode.Ignore else SaveMode.ErrorIfExists
          CreateTableUsingAsSelect(
            tblName,
            hive.conf.defaultDataSourceName,
            temporary = false,
            mode,
            options = Map.empty[String, String],
            child
          )
        } else {
          val databaseName = dbName.getOrElse(hive.sessionState.getCurrentDatabase)
          execution.CreateTableAsSelect(
            databaseName,
            tableName,
            child,
            allowExisting,
            None)
=======
          val desc = if (table.serde.isEmpty) {
            // add default serde
            table.copy(
              serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
          } else {
            table
          }

          val (dbName, tblName) =
            processDatabaseAndTableName(
              desc.specifiedDatabase.getOrElse(client.currentDatabase), desc.name)

          execution.CreateTableAsSelect(
            desc.copy(
              specifiedDatabase = Some(dbName),
              name = tblName),
            child,
            allowExisting)
>>>>>>> upstream/master
        }
    }
  }

  /**
   * Casts input data to correct data types according to table definition before inserting into
   * that table.
   */
  object PreInsertionCasts extends Rule[LogicalPlan] {
    def apply(plan: LogicalPlan): LogicalPlan = plan.transform {
      // Wait until children are resolved.
      case p: LogicalPlan if !p.childrenResolved => p

      case p @ InsertIntoTable(table: MetastoreRelation, _, child, _, _) =>
        castChildOutput(p, table, child)
    }

    def castChildOutput(p: InsertIntoTable, table: MetastoreRelation, child: LogicalPlan)
      : LogicalPlan = {
      val childOutputDataTypes = child.output.map(_.dataType)
      val numDynamicPartitions = p.partition.values.count(_.isEmpty)
      val tableOutputDataTypes =
        (table.attributes ++ table.partitionKeys.takeRight(numDynamicPartitions))
          .take(child.output.length).map(_.dataType)

      if (childOutputDataTypes == tableOutputDataTypes) {
<<<<<<< HEAD
        p
=======
        InsertIntoHiveTable(table, p.partition, p.child, p.overwrite, p.ifNotExists)
>>>>>>> upstream/master
      } else if (childOutputDataTypes.size == tableOutputDataTypes.size &&
        childOutputDataTypes.zip(tableOutputDataTypes)
          .forall { case (left, right) => left.sameType(right) }) {
        // If both types ignoring nullability of ArrayType, MapType, StructType are the same,
        // use InsertIntoHiveTable instead of InsertIntoTable.
<<<<<<< HEAD
        InsertIntoHiveTable(p.table, p.partition, p.child, p.overwrite, p.ifNotExists)
=======
        InsertIntoHiveTable(table, p.partition, p.child, p.overwrite, p.ifNotExists)
>>>>>>> upstream/master
      } else {
        // Only do the casting when child output data types differ from table output data types.
        val castedChildOutput = child.output.zip(table.output).map {
          case (input, output) if input.dataType != output.dataType =>
            Alias(Cast(input, output.dataType), input.name)()
          case (input, _) => input
        }

        p.copy(child = logical.Project(castedChildOutput, child))
      }
    }
  }

  /**
   * UNIMPLEMENTED: It needs to be decided how we will persist in-memory tables to the metastore.
   * For now, if this functionality is desired mix in the in-memory [[OverrideCatalog]].
   */
<<<<<<< HEAD
  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = ???
=======
  override def registerTable(tableIdentifier: Seq[String], plan: LogicalPlan): Unit = {
    throw new UnsupportedOperationException
  }
>>>>>>> upstream/master

  /**
   * UNIMPLEMENTED: It needs to be decided how we will persist in-memory tables to the metastore.
   * For now, if this functionality is desired mix in the in-memory [[OverrideCatalog]].
   */
<<<<<<< HEAD
  override def unregisterTable(tableIdentifier: Seq[String]): Unit = ???
=======
  override def unregisterTable(tableIdentifier: Seq[String]): Unit = {
    throw new UnsupportedOperationException
  }
>>>>>>> upstream/master

  override def unregisterAllTables(): Unit = {}
}

/**
 * A logical plan representing insertion into Hive table.
 * This plan ignores nullability of ArrayType, MapType, StructType unlike InsertIntoTable
 * because Hive table doesn't have nullability for ARRAY, MAP, STRUCT types.
 */
private[hive] case class InsertIntoHiveTable(
<<<<<<< HEAD
    table: LogicalPlan,
=======
    table: MetastoreRelation,
>>>>>>> upstream/master
    partition: Map[String, Option[String]],
    child: LogicalPlan,
    overwrite: Boolean,
    ifNotExists: Boolean)
  extends LogicalPlan {

  override def children: Seq[LogicalPlan] = child :: Nil
  override def output: Seq[Attribute] = child.output

<<<<<<< HEAD
  override lazy val resolved: Boolean = childrenResolved && child.output.zip(table.output).forall {
=======
  val numDynamicPartitions = partition.values.count(_.isEmpty)

  // This is the expected schema of the table prepared to be inserted into,
  // including dynamic partition columns.
  val tableOutput = table.attributes ++ table.partitionKeys.takeRight(numDynamicPartitions)

  override lazy val resolved: Boolean = childrenResolved && child.output.zip(tableOutput).forall {
>>>>>>> upstream/master
    case (childAttr, tableAttr) => childAttr.dataType.sameType(tableAttr.dataType)
  }
}

private[hive] case class MetastoreRelation
    (databaseName: String, tableName: String, alias: Option[String])
    (val table: HiveTable)
    (@transient sqlContext: SQLContext)
  extends LeafNode with MultiInstanceRelation {

  self: Product =>

<<<<<<< HEAD
  override def equals(other: scala.Any): Boolean = other match {
=======
  override def equals(other: Any): Boolean = other match {
>>>>>>> upstream/master
    case relation: MetastoreRelation =>
      databaseName == relation.databaseName &&
        tableName == relation.tableName &&
        alias == relation.alias &&
        output == relation.output
    case _ => false
  }

  override def hashCode(): Int = {
    Objects.hashCode(databaseName, tableName, alias, output)
  }

<<<<<<< HEAD
  // TODO: Can we use org.apache.hadoop.hive.ql.metadata.Table as the type of table and
  // use org.apache.hadoop.hive.ql.metadata.Partition as the type of elements of partitions.
  // Right now, using org.apache.hadoop.hive.ql.metadata.Table and
  // org.apache.hadoop.hive.ql.metadata.Partition will cause a NotSerializableException
  // which indicates the SerDe we used is not Serializable.

  @transient val hiveQlTable: Table = new Table(table)

  @transient val hiveQlPartitions: Seq[Partition] = partitions.map { p =>
    new Partition(hiveQlTable, p)
=======
  @transient val hiveQlTable: Table = {
    // We start by constructing an API table as Hive performs several important transformations
    // internally when converting an API table to a QL table.
    val tTable = new org.apache.hadoop.hive.metastore.api.Table()
    tTable.setTableName(table.name)
    tTable.setDbName(table.database)

    val tableParameters = new java.util.HashMap[String, String]()
    tTable.setParameters(tableParameters)
    table.properties.foreach { case (k, v) => tableParameters.put(k, v) }

    tTable.setTableType(table.tableType.name)

    val sd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor()
    tTable.setSd(sd)
    sd.setCols(table.schema.map(c => new FieldSchema(c.name, c.hiveType, c.comment)))
    tTable.setPartitionKeys(
      table.partitionColumns.map(c => new FieldSchema(c.name, c.hiveType, c.comment)))

    table.location.foreach(sd.setLocation)
    table.inputFormat.foreach(sd.setInputFormat)
    table.outputFormat.foreach(sd.setOutputFormat)

    val serdeInfo = new org.apache.hadoop.hive.metastore.api.SerDeInfo
    sd.setSerdeInfo(serdeInfo)
    table.serde.foreach(serdeInfo.setSerializationLib)
    val serdeParameters = new java.util.HashMap[String, String]()
    serdeInfo.setParameters(serdeParameters)
    table.serdeProperties.foreach { case (k, v) => serdeParameters.put(k, v) }

    new Table(tTable)
  }

  @transient val hiveQlPartitions: Seq[Partition] = table.getAllPartitions.map { p =>
    val tPartition = new org.apache.hadoop.hive.metastore.api.Partition
    tPartition.setDbName(databaseName)
    tPartition.setTableName(tableName)
    tPartition.setValues(p.values)

    val sd = new org.apache.hadoop.hive.metastore.api.StorageDescriptor()
    tPartition.setSd(sd)
    sd.setCols(table.schema.map(c => new FieldSchema(c.name, c.hiveType, c.comment)))

    sd.setLocation(p.storage.location)
    sd.setInputFormat(p.storage.inputFormat)
    sd.setOutputFormat(p.storage.outputFormat)

    val serdeInfo = new org.apache.hadoop.hive.metastore.api.SerDeInfo
    sd.setSerdeInfo(serdeInfo)
    serdeInfo.setSerializationLib(p.storage.serde)

    val serdeParameters = new java.util.HashMap[String, String]()
    serdeInfo.setParameters(serdeParameters)
    table.serdeProperties.foreach { case (k, v) => serdeParameters.put(k, v) }
    p.storage.serdeProperties.foreach { case (k, v) => serdeParameters.put(k, v) }

    new Partition(hiveQlTable, tPartition)
>>>>>>> upstream/master
  }

  @transient override lazy val statistics: Statistics = Statistics(
    sizeInBytes = {
      val totalSize = hiveQlTable.getParameters.get(HiveShim.getStatsSetupConstTotalSize)
      val rawDataSize = hiveQlTable.getParameters.get(HiveShim.getStatsSetupConstRawDataSize)
      // TODO: check if this estimate is valid for tables after partition pruning.
      // NOTE: getting `totalSize` directly from params is kind of hacky, but this should be
      // relatively cheap if parameters for the table are populated into the metastore.  An
      // alternative would be going through Hadoop's FileSystem API, which can be expensive if a lot
      // of RPCs are involved.  Besides `totalSize`, there are also `numFiles`, `numRows`,
      // `rawDataSize` keys (see StatsSetupConst in Hive) that we can look at in the future.
      BigInt(
        // When table is external,`totalSize` is always zero, which will influence join strategy
        // so when `totalSize` is zero, use `rawDataSize` instead
        // if the size is still less than zero, we use default size
        Option(totalSize).map(_.toLong).filter(_ > 0)
          .getOrElse(Option(rawDataSize).map(_.toLong).filter(_ > 0)
          .getOrElse(sqlContext.conf.defaultSizeInBytes)))
    }
  )

  /** Only compare database and tablename, not alias. */
  override def sameResult(plan: LogicalPlan): Boolean = {
    plan match {
      case mr: MetastoreRelation =>
        mr.databaseName == databaseName && mr.tableName == tableName
      case _ => false
    }
  }

  val tableDesc = HiveShim.getTableDesc(
    Class.forName(
      hiveQlTable.getSerializationLib,
      true,
      Utils.getContextOrSparkClassLoader).asInstanceOf[Class[Deserializer]],
    hiveQlTable.getInputFormatClass,
    // The class of table should be org.apache.hadoop.hive.ql.metadata.Table because
    // getOutputFormatClass will use HiveFileFormatUtils.getOutputFormatSubstitute to
    // substitute some output formats, e.g. substituting SequenceFileOutputFormat to
    // HiveSequenceFileOutputFormat.
    hiveQlTable.getOutputFormatClass,
    hiveQlTable.getMetadata
  )

<<<<<<< HEAD
  implicit class SchemaAttribute(f: FieldSchema) {
    def toAttribute: AttributeReference = AttributeReference(
      f.getName,
      HiveMetastoreTypes.toDataType(f.getType),
=======
  implicit class SchemaAttribute(f: HiveColumn) {
    def toAttribute: AttributeReference = AttributeReference(
      f.name,
      HiveMetastoreTypes.toDataType(f.hiveType),
>>>>>>> upstream/master
      // Since data can be dumped in randomly with no validation, everything is nullable.
      nullable = true
    )(qualifiers = Seq(alias.getOrElse(tableName)))
  }

  /** PartitionKey attributes */
  val partitionKeys = table.partitionColumns.map(_.toAttribute)

  /** Non-partitionKey attributes */
  val attributes = table.schema.map(_.toAttribute)

  val output = attributes ++ partitionKeys

  /** An attribute map that can be used to lookup original attributes based on expression id. */
  val attributeMap = AttributeMap(output.map(o => (o, o)))

  /** An attribute map for determining the ordinal for non-partition columns. */
  val columnOrdinals = AttributeMap(attributes.zipWithIndex)

  override def newInstance(): MetastoreRelation = {
<<<<<<< HEAD
    MetastoreRelation(databaseName, tableName, alias)(table, partitions)(sqlContext)
=======
    MetastoreRelation(databaseName, tableName, alias)(table)(sqlContext)
>>>>>>> upstream/master
  }
}


private[hive] object HiveMetastoreTypes {
<<<<<<< HEAD
  def toDataType(metastoreType: String): DataType = DataTypeParser(metastoreType)
=======
  def toDataType(metastoreType: String): DataType = DataTypeParser.parse(metastoreType)
>>>>>>> upstream/master

  def toMetastoreType(dt: DataType): String = dt match {
    case ArrayType(elementType, _) => s"array<${toMetastoreType(elementType)}>"
    case StructType(fields) =>
      s"struct<${fields.map(f => s"${f.name}:${toMetastoreType(f.dataType)}").mkString(",")}>"
    case MapType(keyType, valueType, _) =>
      s"map<${toMetastoreType(keyType)},${toMetastoreType(valueType)}>"
    case StringType => "string"
    case FloatType => "float"
    case IntegerType => "int"
    case ByteType => "tinyint"
    case ShortType => "smallint"
    case DoubleType => "double"
    case LongType => "bigint"
    case BinaryType => "binary"
    case BooleanType => "boolean"
    case DateType => "date"
    case d: DecimalType => HiveShim.decimalMetastoreString(d)
    case TimestampType => "timestamp"
    case NullType => "void"
    case udt: UserDefinedType[_] => toMetastoreType(udt.sqlType)
  }
}
