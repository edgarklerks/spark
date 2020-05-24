package org.apache.spark.sql.glue.conversions

import java.util.Date

import com.amazonaws.services.glue.model.TableInput
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.glue.conversions.Conversion.GlueColumns
import scala.collection.JavaConverters._

object CatalogTableToTableInput extends Conversion[CatalogTable,TableInput] {
  override def apply(catalogTable: CatalogTable): TableInput = {


    /**
     * Computes the columns, partitionColumns and the bucketColumns
     */
    val glueColumns  = GlueColumns(catalogTable.schema,catalogTable.bucketSpec,catalogTable.partitionColumnNames,catalogTable.storage)

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
      .withStorageDescriptor(GlueColumnToStorageDescriptor(glueColumns))
      .withTableType(catalogTable.tableType.name)
    tblInput      }
}
