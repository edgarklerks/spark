package org.apache.spark.sql.glue.conversions

import java.net.URI

import com.amazonaws.services.glue.model.StorageDescriptor
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import scala.collection.JavaConverters._

object StorageDescriptorToCatalogStorageFormat extends Conversion[StorageDescriptor, CatalogStorageFormat] {
  override def apply(storageDescriptor: StorageDescriptor): CatalogStorageFormat = {
    new CatalogStorageFormat(
      locationUri = Some(URI.create(storageDescriptor.getLocation)),
      inputFormat = Some(storageDescriptor.getInputFormat),
      outputFormat = Some(storageDescriptor.getOutputFormat),
      serde = Some(storageDescriptor.getSerdeInfo.getSerializationLibrary),
      compressed = storageDescriptor.getCompressed,
      properties = storageDescriptor.getParameters.asScala.toMap
    )
  }
 }
