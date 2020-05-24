package org.apache.spark.sql.glue.conversions

import com.amazonaws.services.glue.model.StorageDescriptor
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat

object StorageDescriptorToCatalogStorageFormat extends Conversion[StorageDescriptor, CatalogStorageFormat] {
  override def apply(x: StorageDescriptor): CatalogStorageFormat = ???
}
