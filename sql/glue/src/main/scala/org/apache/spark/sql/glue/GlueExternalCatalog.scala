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

import java.net.URI
import java.util.Date

import com.amazonaws.services.glue.AWSGlueClientBuilder
import com.amazonaws.services.glue.model.{AlreadyExistsException, BatchDeleteTableRequest, Column, CreateDatabaseRequest, CreateTableRequest, Database, DatabaseInput, DeleteDatabaseRequest, DeleteTableRequest, EntityNotFoundException, GetDatabaseRequest, GetDatabaseResult, GetDatabasesRequest, GetDatabasesResult, GetTableRequest, GetTablesRequest, GetTablesResult, SerDeInfo, StorageDescriptor, Table, TableInput, UpdateDatabaseRequest, UpdateTableRequest}
import com.amazonaws.services.securitytoken.AWSSecurityTokenServiceClientBuilder
import com.amazonaws.services.securitytoken.model.GetCallerIdentityRequest
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.analysis.{DatabaseAlreadyExistsException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogDatabase, CatalogFunction, CatalogStatistics, CatalogStorageFormat, CatalogTable, CatalogTablePartition, ExternalCatalog}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.StructType

import scala.annotation.tailrec
import scala.collection.JavaConverters._

private[spark] object GlueExternalCatalog {
  /**
   * Used to set an explicit catalog id. If not set in the options then the current aws account id is assumed.
   */
  private val catalogIdOption = "spark.sql.metastore.glue.catalogid"
  /**
   * The clients should be thread safe, so can be shared.
   */
  private val sts = AWSSecurityTokenServiceClientBuilder.defaultClient()
  private val glue = AWSGlueClientBuilder.defaultClient()

  /**
   * Glue is a rest API so we keep the current database  we are working on locally.
   */
  private val currentDatabase = new ThreadLocal[Option[String]] {
    override def initialValue(): Option[String] = None
  }

  /**
   * Get the current database unless the db parameter is set then return that.
   * Asserts that one of them is set
   * @param db database that overrides the current database setting
   * @return a database name
   */
  private def getCurrentDatabase(db: Option[String]): String = {
    val dbName = db.orElse(currentDatabase.get())
    assert(dbName.isDefined)
    dbName.get
  }

  /**
   * Intermediate type that combines all the information needed to create a TableInput
   * @param schema a Structype representing the schema of the table
   * @param bucketSpec a bucket specification, may be null
   * @param partitionColumnNames the names of the partition columns
   */
  case class GlueColumns(private val schema: StructType, private val bucketSpec: Option[BucketSpec], private val partitionColumnNames : Seq[String]) {
    val hasBucketSpec = bucketSpec.isDefined
    /** Set to safe values we can always feed to glue without hurting ourselves  */
    val numberOfBucketColumns = bucketSpec.map(_.numBuckets).getOrElse(0)
    /** Bucket columns may be empty */
    val bucketColumns: Seq[String] = bucketSpec.toSeq.flatMap(_.bucketColumnNames)
    /**
     * Not sure yet if spark and glue are fully compatible, so perhaps not all columns are supported.
     * TODO: Needs to be tested
     */
    private val allColumns = schema.map(col => {
      new Column()
        .withName(col.name)
        .withType(col.dataType.catalogString)
    })
    val columns = allColumns.filterNot(partitionColumnNames.contains(_))
    /**
     * In glue it seems the partition columns are not part of the normal columns, so they need to be divided.
     * Also the partition columns need to carry their type, because they are not part of the normal columns
     */
    val partitionKeys = allColumns.filterNot(partitionColumnNames.contains(_))
  }


  /**
   * Converts a [[CatalogStorageFormat]] together with the columns ([[GlueColumns]] into a [[StorageDescriptor]]
   * @param catalogStorageFormat the storage format of the table
   * @param glueColumns the columns of the table
   * @return a storage descriptor of the table
   */
  private def storageToStorageDescriptor(catalogStorageFormat: CatalogStorageFormat, glueColumns: GlueColumns) : StorageDescriptor = {

   val storageDescriptor =  new StorageDescriptor()
        .withColumns(glueColumns.columns.asJavaCollection)
        .withCompressed(catalogStorageFormat.compressed)

    if(glueColumns.hasBucketSpec){
      storageDescriptor.setBucketColumns(glueColumns.bucketColumns.asJavaCollection)
      storageDescriptor.setNumberOfBuckets(glueColumns.numberOfBucketColumns)
    }

    catalogStorageFormat.inputFormat match {
      case None =>
      case Some(inp) => storageDescriptor.setInputFormat(inp)
    }

    catalogStorageFormat.locationUri match {
      case None =>
      case Some(uri) => storageDescriptor.setLocation(uri.toASCIIString)
    }
    catalogStorageFormat.outputFormat match {
      case None =>
      case Some(out) => storageDescriptor.setOutputFormat(out)
    }

    /**
     * For serde it is not needed to set all the properties, {{{catalogStorageFormat.serde}}} maps to the name of the serialization library used.
     */
    catalogStorageFormat.serde match {
      case None =>
      case Some(serdeLibName) => storageDescriptor.withSerdeInfo(new SerDeInfo().withSerializationLibrary(serdeLibName))
    }

    storageDescriptor
      .withParameters(catalogStorageFormat.properties.asJava)
  }

  /**
   * Transforms a [[CatalogTable]] into a [[TableInput]].
   * @param catalogTable
   * @return a tableInput describing the table
   */
  private def catalogTableToTableInput(catalogTable: CatalogTable) : TableInput = {

    /**
     * Computes the columns, partitionColumns and the bucketColumns
     */
    val glueColumns  = GlueColumns(catalogTable.schema,catalogTable.bucketSpec,catalogTable.partitionColumnNames)

    /**
     * withRetention and withLastAnalyzedTime are not set, because there doesn't seem to be anything in [[CatalogTable]] that I can map
     */
    val tblInput = new TableInput()
      .withName(catalogTable.identifier.table)
      /**
       * I guess ignored parameters can be passed on? Perhaps I need to make a namespace for them spark.sql.glue
       * TODO: Check if this causes problems
       * */
      .withParameters(catalogTable.ignoredProperties.asJava)
      .withLastAccessTime(new Date(catalogTable.lastAccessTime))
      .withPartitionKeys(glueColumns.partitionKeys.asJavaCollection)
      .withStorageDescriptor(storageToStorageDescriptor(catalogTable.storage, glueColumns))
      .withTableType(catalogTable.tableType.name)
    tblInput
  }

  /**
   * Table and TableInput contain the same information, but of course there is no method to convert between them.
   * Amazon seems to generate their SDK's from their rest API, which explains a couple of things (like the several redefinitions of the
   * idea of a Tag throughout the aws sdk).
   * @param table
   * @return
   */
  private def tableToTableInput(table : Table) : TableInput = {
    new TableInput()
      .withDescription(table.getDescription)
      .withLastAccessTime(table.getLastAccessTime)
      .withLastAnalyzedTime(table.getLastAnalyzedTime)
      .withName(table.getName)
      .withOwner(table.getOwner)
      .withParameters(table.getParameters)
      .withPartitionKeys(table.getPartitionKeys)
      .withRetention(table.getRetention)
      .withStorageDescriptor(table.getStorageDescriptor)
      .withTableType(table.getTableType)
      .withViewExpandedText(table.getViewExpandedText)
      .withViewOriginalText(table.getViewOriginalText)
  }

  /**
   * Amazon has forgotten to create iterables for its types, so you have to write the same code to loop over
   * pages returned by the API over and over again.
   * This resolves by exploiting that every type Amazon uses, has an implicit interface. This interface is expressed by Q.
   * Unfortunately, every object has its own method name to get the values you are interested in.
   *
   * Except for S3, because that has a slightly different structure, so it doesn't fit the Q type
   *
   * @tparam V The result type, e.g. if {{{ type S = GetTablesResult }}} then {{{ type V = Table }}}
   */
  private trait ToIterable[V] {
    /**
     * Q is the supertype of S, it defines that S at least should have the following two methods:
     */
    type Q = {
      def getNextToken() : String
      def withNextToken(token: String) : S
    }

    /**
     * Tell the compiler to check that S is a subtype of Q
     * in java language: it means S has the interface defined above (getNextToken...)
     */
    type S <: Q

    /**
     * Use getV to get one page, then ask for a new token and recurse if not null, otherwise return results
     * Call is tail recursive
     *
     * @param q  the thing S containing V
     * @param xs the already collected results (not for the user to call)
     * @return
     */
    @tailrec
    final def apply(q: S, xs: Iterable[V] = Iterable.empty): Iterable[V] = {
      val res = getV(q)
      val newToken = q.getNextToken()
      if (newToken != null) apply(q.withNextToken(newToken), res ++ xs) else res ++ xs
    }

    /**
     * Method the user should implement, it describe how to get one page of items.
     *
     * @param s thing S containing some V's
     * @return one page of iterable V
     */
    protected def getV(s: S): Iterable[V]
  }

  private val getTablesIterable = new ToIterable[Table] {
    override type S = GetTablesResult
    override protected def getV(s: GetTablesResult): Iterable[Table] = s.getTableList.asScala
  }
  private val getDatabasesIterable = new ToIterable[Database] {
    override type S = GetDatabasesResult
    override protected def getV(s: GetDatabasesResult): Iterable[Database] = s.getDatabaseList.asScala

  }
}
private[spark] class GlueExternalCatalog(conf : SparkConf) extends ExternalCatalog {
  import GlueExternalCatalog._

  private lazy val catalogId = conf.getOption(catalogIdOption)
    .getOrElse(
      sts.getCallerIdentity(
        new GetCallerIdentityRequest()
      ).getAccount
    )

  override def createDatabase(
                               dbDefinition: CatalogDatabase,
                               ignoreIfExists: Boolean): Unit =
    try {

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
    } catch {
      case e : AlreadyExistsException => {
        if(!ignoreIfExists){
            throw new DatabaseAlreadyExistsException(dbDefinition.name)
        }
      }
    }

  private def getTables(db : String, expression : Option[String]) : Iterable[Table] = {
    val getTablesRequest = (expression  match {
      case Some(expr) =>
        new GetTablesRequest().withExpression(expr)
      case None => new GetTablesRequest()
    }).withCatalogId(catalogId).withDatabaseName(db)
    getTablesIterable(glue.getTables(getTablesRequest))
  }

  /**
   * Drop databases drops the database if it is empty or drops it with all tables if cascade is set.
   * Note that glue won't delete data. So the data will still be available.
   * @param db Database name
   * @param ignoreIfNotExists if this is set, won't throw exception when database doesn't exist
   * @param cascade if set removes first all table definitions before removing database
   */
  override def dropDatabase(
                             db: String,
                             ignoreIfNotExists: Boolean,
                             cascade: Boolean): Unit = {
    if(!ignoreIfNotExists){
      requireDbExists(db)
    }

    /**
     * Doing a batch delete here, instead of calling dropTable one by one.
     */
    if(cascade){
      val tables = this.getTables(db, None).map(_.getName)
      glue.batchDeleteTable(
        new BatchDeleteTableRequest()
          .withCatalogId(catalogId)
          .withDatabaseName(db)
          .withTablesToDelete(tables.asJavaCollection)
      )
    }
      glue.deleteDatabase(
        new DeleteDatabaseRequest()
          .withCatalogId(catalogId)
          .withName(db)
      )
  }

  /**
   * Alter a database whose name matches the one specified in `dbDefinition`,
   * assuming the database exists.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  override def alterDatabase(dbDefinition: CatalogDatabase): Unit = {

    requireDbExists(dbDefinition.name)

    glue.updateDatabase(
      new UpdateDatabaseRequest()
        .withCatalogId(catalogId)
        .withName(dbDefinition.name)
        .withDatabaseInput(
          new DatabaseInput()
            .withName(dbDefinition.name)
            .withDescription(dbDefinition.description)
            .withLocationUri(dbDefinition.locationUri.toASCIIString)
            .withParameters(dbDefinition.properties.asJava)
        )
    )
  }

  override def getDatabase(db: String): CatalogDatabase = {
    requireDbExists(db)

    val dbResult = glue.getDatabase(new GetDatabaseRequest().withCatalogId(catalogId).withName(db)).getDatabase
    CatalogDatabase(dbResult.getName,dbResult.getDescription,URI.create(dbResult.getLocationUri),dbResult.getParameters.asScala.toMap)
  }

  override def databaseExists(db: String): Boolean = {
    try {
      glue.getDatabase(new GetDatabaseRequest()
        .withCatalogId(catalogId)
        .withName(db)
      )
      true
    } catch {
      case e : EntityNotFoundException => false
    }
  }

  private def getDatabases() : Iterable[Database] = {
    val databases = glue.getDatabases(new GetDatabasesRequest().withCatalogId(catalogId))
    getDatabasesIterable(databases)
  }
  override def listDatabases(): Seq[String] = {
    getDatabases().map(_.getName).toSeq
  }

  /**
   * If pattern is found in the name of a database, include it in the listing.
   * @param pattern
   * @return
   */
  override def listDatabases(pattern: String): Seq[String] = {
    listDatabases().filter(q => q.contains(pattern))
  }


  /**
   * Sets the current database, because this is not supported by glue, it is stored thread local.
   * @param db database name
   */
  override def setCurrentDatabase(db: String): Unit = {
    requireDbExists(db)

    currentDatabase.set(Some(db))
  }

  /**
   * Creates a table as specified by _tableDefinition_ or if it already exists and
   * {{{ignoreIfExists == false}}} throw exception [[TableAlreadyExistsException]]
   * @param tableDefinition Definition of table
   * @param ignoreIfExists Set to simulate `CREATE TABLE IF NOT EXISTS`
   */
  override def createTable(tableDefinition: CatalogTable, ignoreIfExists: Boolean): Unit = {
    val db = getCurrentDatabase(tableDefinition.identifier.database)
    requireDbExists(db)

    try {
     glue.createTable(
      new CreateTableRequest()
        .withCatalogId(catalogId)
        .withDatabaseName(db)
        .withTableInput(catalogTableToTableInput(tableDefinition))
    )
    } catch {
      case _ : AlreadyExistsException => {
        if(!ignoreIfExists){
          throw new TableAlreadyExistsException(db,tableDefinition.identifier.table)
        }
      }

    }
  }

  /**
   * Drops the table definition from the database table. Note that glue *won't* delete the data. This also means purge won't do
   * anything.
   * @param db name fo the database the table resides in
   * @param table name of the table to be deleted
   * @param ignoreIfNotExists do not throw exceptions if table doesn't exist
   * @param purge ignored
   */
  override def dropTable(db: String, table: String, ignoreIfNotExists: Boolean, purge: Boolean): Unit = {
    if(!ignoreIfNotExists){
      requireDbExists(db)
      requireTableExists(db,table)
    }
    glue.deleteTable(new DeleteTableRequest().withCatalogId(catalogId).withDatabaseName(db).withName(table))
  }

  override def renameTable(db: String, oldName: String, newName: String): Unit = {
    requireDbExists(db)
    requireTableExists(db, oldName)

    val table : Table = glue.getTable(new GetTableRequest().withCatalogId(catalogId).withDatabaseName(db).withName(newName)).getTable

    glue.updateTable(new UpdateTableRequest().withCatalogId(catalogId).withDatabaseName(db).withTableInput(
      tableToTableInput(table).withName(newName)
    ))
  }

  /**
   * Alter a table whose database and name match the ones specified in `tableDefinition`, assuming
   * the table exists. Note that, even though we can specify database in `tableDefinition`, it's
   * used to identify the table, not to alter the table's database, which is not allowed.
   *
   * Note: If the underlying implementation does not support altering a certain field,
   * this becomes a no-op.
   */
  override def alterTable(tableDefinition: CatalogTable): Unit = {
    val db = getCurrentDatabase(tableDefinition.identifier.database)
    requireDbExists(db)
    requireTableExists(db, tableDefinition.identifier.table)

    glue.updateTable(new UpdateTableRequest().withCatalogId(catalogId).withDatabaseName(db).withTableInput(catalogTableToTableInput(tableDefinition)))

  }

  /**
   * Alter the data schema of a table identified by the provided database and table name. The new
   * data schema should not have conflict column names with the existing partition columns, and
   * should still contain all the existing data columns.
   *
   * @param db            Database that table to alter schema for exists in
   * @param table         Name of table to alter schema for
   * @param newDataSchema Updated data schema to be used for the table.
   */
  override def alterTableDataSchema(db: String, table: String, newDataSchema: StructType): Unit = {
    requireDbExists(db)
    requireTableExists(db, table)

    val tableDefinition = getTable(db, table).copy(schema = newDataSchema)

    glue.updateTable(new UpdateTableRequest().withCatalogId(catalogId).withDatabaseName(db).withTableInput(catalogTableToTableInput(tableDefinition)))



  }

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
