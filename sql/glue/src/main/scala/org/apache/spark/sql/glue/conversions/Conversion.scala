package org.apache.spark.sql.glue.conversions

import java.util.Date

import com.amazonaws.services.glue.model.{Column, SerDeInfo, StorageDescriptor, Table, TableInput}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogStorageFormat, CatalogTable}
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

trait Conversion[A,B] {
  def apply(x : A) : B

}
object Conversion {
    def convert[A,B](x : A)(implicit conversion : Conversion[A,B]) : B = conversion(x)

  /**
   * Intermediate type that combines all the information needed to create a TableInput
   * @param schema a Structype representing the schema of the table
   * @param bucketSpec a bucket specification, may be null
   * @param partitionColumnNames the names of the partition columns
   */
  case class GlueColumns(private val schema: StructType, private val bucketSpec: Option[BucketSpec], private val partitionColumnNames : Seq[String], val storage : CatalogStorageFormat) {
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



  private def storageDescriptorToCatalogStorageFormat(storageDescriptor: StorageDescriptor) : CatalogStorageFormat = {
    ???
  }

}
