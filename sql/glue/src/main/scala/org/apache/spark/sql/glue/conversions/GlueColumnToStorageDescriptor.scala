package org.apache.spark.sql.glue.conversions

import com.amazonaws.services.glue.model.{SerDeInfo, StorageDescriptor}
import org.apache.spark.sql.catalyst.catalog.CatalogStorageFormat
import org.apache.spark.sql.glue.conversions.Conversion.GlueColumns
import scala.collection.JavaConverters._

object GlueColumnToStorageDescriptor extends Conversion[GlueColumns, StorageDescriptor]{
  override def apply(glueColumns: GlueColumns): StorageDescriptor = {
    val catalogStorageFormat = glueColumns.storage

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

}
