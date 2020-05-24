package org.apache.spark.sql.glue.conversions

import com.amazonaws.services.glue.model.Table
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.types.{DataType, StructField, StructType}

import scala.collection.JavaConverters._

object TableToCatalogTable extends Conversion[Table, CatalogTable] {
  def deriveSchema(table : Table) : StructType = {
    val partitionKeys = table.getPartitionKeys.asScala
    val columns = table.getStorageDescriptor.getColumns.asScala

    val schemaTypes = columns.map(col => StructField(col.getName, DataType.fromDDL(col.getType))) ++
                      partitionKeys.map(col => StructField(col.getName,DataType.fromDDL(col.getType)))

    StructType(schemaTypes)
  }
  override def apply(table: Table): CatalogTable = {
    new CatalogTable(
      identifier = new TableIdentifier(table = table.getName,database = Some(table.getDatabaseName)),
      tableType = CatalogTableType.apply(table.getTableType),
      storage = StorageDescriptorToCatalogStorageFormat(table.getStorageDescriptor),
      schema = deriveSchema(table)
    )
  }
}
