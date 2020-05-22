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
package org.apache.spark.sql.glue

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.services.glue.AWSGlueClientBuilder
import com.amazonaws.services.glue.model.{CreateDatabaseRequest, DatabaseInput}
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogFunction, CatalogStatistics, CatalogTable, CatalogTablePartition, ExternalCatalog}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._

object GlueExternalCatalog {
  val catalogIdOption = "spark.sql.metastore.glue.catalogid"
}
private[spark] class GlueExternalCatalog(conf : SparkConf) extends ExternalCatalog {
   private val credentials = DefaultAWSCredentialsProviderChain.getInstance()
   private val sts = AWSSecurityTokenServiceClientBuilder.defaultClient()
   private val glue  = AWSGlueClientBuilder.defaultClient()
   private val catalogId = conf.getOption(catalogId)
     .getOrElse(
       sts.getCallerIdentity(
         new GetCallerIdentityRequest()
       ).getAccount
     )

  override def createDatabase(dbDefinition: CatalogDatabase, ignoreIfExists: Boolean): Unit = {
    glue.createDatabase(
      new CreateDatabaseRequest()
        .withCatalogId(catalogId)
        .withDatabaseInput(
          new DatabaseInput()
            .withName(dbDefinition.name)
            .withLocationUri(dbDefinition.locationUri.toASCIIString)
            .withParameters(dbDefinition.properties.asJava)
            .withDescription(dbDefinition.description)
        )
    )
  }

  override def dropDatabase(db: String, ignoreIfNotExists: Boolean, cascade: Boolean): Unit = ???

  /**
   * Alter a database whose name matches the one specified in `dbDefinition`,
   * assuming the database exists.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = ???

  override def getDatabase(db: String): CatalogDatabase = ???

  override def databaseExists(db: String): Boolean = ???

  override def listDatabases(): Seq[String] = ???

  override def listDatabases(pattern: String): Seq[String] = ???

  override def setCurrentDatabase(db: String): Unit = ???

  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = ???

  override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = ???

  override def renameTable(db: String, oldName: String, newName: String): Unit = ???

  /**
   * Alter a table whose database and name match the ones specified in `tableDefinition`, assuming
   * the table exists. Note that, even though we can specify database in `tableDefinition`, it's
   * used to identify the table, not to alter the table's database, which is not allowed.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  override def alterTable(tableDefinition: CatalogTable): Unit = ???

  /**
   * Alter the data schema of a table identified by the provided database and table name. The new
   * data schema should not have conflict column names with the existing partition columns, and
   * should still contain all the existing data columns.
   *
   * @param db            Database that table to alter schema for exists in
   * @param table         Name of table to alter schema for
   * @param newDataSchema Updated data schema to be used for the table.
   */
  override def alterTableDataSchema(db: String, table: String, newDataSchema: StructType): Unit = ???

  /** Alter the statistics of a table. If `stats` is None, then remove all existing statistics. */
  override def alterTableStats(db: String, table: String, stats: Option[CatalogStatistics]): Unit = ???

  override def getTable(db: String, table: String): CatalogTable = ???

  override def getTablesByName(db: String, tables: Seq[String]): Seq[CatalogTable] = ???

  override def tableExists(db: String, table: String): Boolean = ???

  override def listTables(db: String): Seq[String] = ???

  override def listTables(db: String, pattern: String): Seq[String] = ???

  override def listViews(db: String, pattern: String): Seq[String] = ???

  /**
   * Loads data into a table.
   *
   * @param isSrcLocal Whether the source data is local, as defined by the "LOAD DATA LOCAL"
   *                   HiveQL command.
   */
  override def loadTable(db: String, table: String, loadPath: String, isOverwrite: Boolean, isSrcLocal: Boolean): Unit = ???

  /**
   * Loads data into a partition.
   *
   * @param isSrcLocal Whether the source data is local, as defined by the "LOAD DATA LOCAL"
   *                   HiveQL command.
   */
  override def loadPartition(db: String, table: String, loadPath: String, partition: TablePartitionSpec, isOverwrite: Boolean, inheritTableSpecs: Boolean, isSrcLocal: Boolean): Unit = ???

  override def loadDynamicPartitions(db: String, table: String, loadPath: String, partition: TablePartitionSpec, replace: Boolean, numDP: Int): Unit = ???

  override def createPartitions(db: String, table: String, parts: Seq[CatalogTablePartition], ignoreIfExists: Boolean): Unit = ???

  override def dropPartitions(db: String, table: String, parts: Seq[TablePartitionSpec], ignoreIfNotExists: Boolean, purge: Boolean, retainData: Boolean): Unit = ???

  /**
   * Override the specs of one or many existing table partitions, assuming they exist.
   * This assumes index i of `specs` corresponds to index i of `newSpecs`.
   */
  override def renamePartitions(db: String, table: String, specs: Seq[TablePartitionSpec], newSpecs: Seq[TablePartitionSpec]): Unit = ???

  /**
   * Alter one or many table partitions whose specs that match those specified in `parts`,
   * assuming the partitions exist.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  override def alterPartitions(db: String, table: String, parts: Seq[CatalogTablePartition]): Unit = ???

  override def getPartition(db: String, table: String, spec: TablePartitionSpec): CatalogTablePartition = ???

  /**
   * Returns the specified partition or None if it does not exist.
   */
  override def getPartitionOption(db: String, table: String, spec: TablePartitionSpec): Option[CatalogTablePartition] = ???

  /**
   * List the names of all partitions that belong to the specified table, assuming it exists.
   *
   * For a table with partition columns p1, p2, p3, each partition name is formatted as
   * `p1=v1/p2=v2/p3=v3`. Each partition column name and value is an escaped path name, and can be
   * decoded with the `ExternalCatalogUtils.unescapePathName` method.
   *
   * The returned sequence is sorted as strings.
   *
   * A partial partition spec may optionally be provided to filter the partitions returned, as
   * described in the `listPartitions` method.
   *
   * @param db          database name
   * @param table       table name
   * @param partialSpec partition spec
   */
  override def listPartitionNames(db: String, table: String, partialSpec: Option[TablePartitionSpec]): Seq[String] = ???

  /**
   * List the metadata of all partitions that belong to the specified table, assuming it exists.
   *
   * A partial partition spec may optionally be provided to filter the partitions returned.
   * For instance, if there exist partitions (a='1', b='2'), (a='1', b='3') and (a='2', b='4'),
   * then a partial spec of (a='1') will return the first two only.
   *
   * @param db          database name
   * @param table       table name
   * @param partialSpec partition spec
   */
  override def listPartitions(db: String, table: String, partialSpec: Option[TablePartitionSpec]): Seq[CatalogTablePartition] = ???

  /**
   * List the metadata of partitions that belong to the specified table, assuming it exists, that
   * satisfy the given partition-pruning predicate expressions.
   *
   * @param db                database name
   * @param table             table name
   * @param predicates        partition-pruning predicates
   * @param defaultTimeZoneId default timezone id to parse partition values of TimestampType
   */
  override def listPartitionsByFilter(db: String, table: String, predicates: Seq[Expression], defaultTimeZoneId: String): Seq[CatalogTablePartition] = ???

  override def createFunction(db: String, funcDefinition: CatalogFunction): Unit = ???

  override def dropFunction(db: String, funcName: String): Unit = ???

  override def alterFunction(db: String, funcDefinition: CatalogFunction): Unit = ???

  override def renameFunction(db: String, oldName: String, newName: String): Unit = ???

  override def getFunction(db: String, funcName: String): CatalogFunction = ???

  override def functionExists(db: String, funcName: String): Boolean = ???

  override def listFunctions(db: String, pattern: String): Seq[String] = ???
}
