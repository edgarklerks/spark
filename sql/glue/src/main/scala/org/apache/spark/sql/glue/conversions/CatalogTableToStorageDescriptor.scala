package org.apache.spark.sql.glue.conversions

import com.amazonaws.services.glue.model.{StorageDescriptor}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable}
import org.apache.spark.sql.glue.conversions.Conversion.GlueColumns


object CatalogTableToStorageDescriptor extends Conversion[CatalogTable, StorageDescriptor]{
  override def apply(catalogTable: CatalogTable): StorageDescriptor = {
    val glueColumns  = GlueColumns(catalogTable.schema,catalogTable.bucketSpec,catalogTable.partitionColumnNames,catalogTable.storage)
    GlueColumnToStorageDescriptor(glueColumns)
  }

}
